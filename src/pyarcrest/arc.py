"""
Module for interaction with the ARC CE REST interface.

Automatic support for multiple versions of the API is implemented with optional
manual selection of the API version. This is done by defining a base class with
methods closely reflecting the operations specified in the ARC CE REST
interface specification: https://www.nordugrid.org/arc/arc6/tech/rest/rest.html
Additionally, the base class defines some higher level methods, e. g. a method
to upload job input files using multiple threads.

Some operations involved in determining the API version are implemented in class
methods instead of instance methods as instance methods are considered to be
tied to the API version. Determination of API version should therefore be a
static operation.
"""


import concurrent.futures
import datetime
import json
import os
import queue
import sys
import threading
from http.client import HTTPException
from urllib.parse import urlparse

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from pyarcrest.common import getNullLogger, HTTP_BUFFER_SIZE
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              MatchmakingError, MissingDiagnoseFile,
                              MissingOutputFile, NoValueInARCResult)
from pyarcrest.http import HTTPClient
from pyarcrest.x509 import parsePEM, signRequest


# TODO: blocksize can only be used with Python >= 3.7


class ARCRest:

    def __init__(self, httpClient, apiBase="/arex", logger=getNullLogger()):
        """
        Initialize the base object.

        Note that this class should not be instantiated directly because
        additional implementations of attributes and methods are required from
        derived classes.
        """
        self.logger = logger
        self.apiBase = apiBase
        self.httpClient = httpClient

    def close(self):
        self.httpClient.close()

    ### Direct operations on ARC CE ###

    def getAPIVersions(self):
        return self.getAPIVersionsStatic(self.httpClient, self.apiBase)

    def getCEInfo(self):
        status, text = self._requestJSON("GET", f"{self.apiPath}/info")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting ARC CE info: {status} {text}")
        return json.loads(text)

    def getJobsList(self):
        status, text = self._requestJSON("GET", f"{self.apiPath}/jobs")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting jobs list: {status} {text}")

        # /rest/1.0 compatibility
        try:
            jsonData = json.loads(text)["job"]
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                jsonData = []
            else:
                raise
        if isinstance(jsonData, dict):
            jsonData = [jsonData]

        return [ARCJob(job["id"]) for job in jsonData]

    def createJobs(self, jobs, delegationID, queue):
        raise Exception("Not implemented in the base class")

    def getJobsInfo(self, jobs):
        results = self._manageJobs(jobs, "info")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"Error getting info for job {job.id}: {code} {reason}"))
            elif "info_document" not in result:
                job.errors.append(NoValueInARCResult(f"No info document in successful info response for job {job.id}"))
            else:
                job.updateFromInfo(result["info_document"])

    def getJobsStatus(self, jobs):
        results = self._manageJobs(jobs, "status")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"Error getting status for job {job.id}: {code} {reason}"))
            elif "state" not in result:
                job.errors.append(NoValueInARCResult("No state in successful status response"))
            else:
                job.state = result["state"]

    def killJobs(self, jobs):
        results = self._manageJobs(jobs, "kill")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 202:
                job.errors.append(ARCHTTPError(code, reason, f"Error killing job {job.id}: {code} {reason}"))

    def cleanJobs(self, jobs):
        results = self._manageJobs(jobs, "clean")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 202:
                job.errors.append(ARCHTTPError(code, reason, f"Error cleaning job {job.id}: {code} {reason}"))

    def restartJobs(self, jobs):
        results = self._manageJobs(jobs, "restart")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 202:
                job.errors.append(ARCHTTPError(code, reason, f"Error restarting job {job.id}: {code} {reason}"))

    def getJobsDelegations(self, jobs):
        results = self._manageJobs(jobs, "delegations")
        for job, result in zip(jobs, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 200:
                job.errors.append(ARCHTTPError(code, reason, f"Error getting delegations for job {job.id}: {code} {reason}"))
            elif "delegation_id" not in result:
                job.errors.append(NoValueInARCResult("No delegation ID in successful response"))
            else:
                job.delegations = result["delegation_id"]
                # /rest/1.0 compatibility
                if not isinstance(job.delegations, list):
                    job.delegations = [job.delegations]

    def downloadFile(self, url, path, blocksize=None):
        if blocksize is None:
            blocksize = HTTP_BUFFER_SIZE

        resp = self.httpClient.request("GET", url)

        if resp.status != 200:
            text = resp.read().decode()
            raise ARCHTTPError(resp.status, text, f"Error downloading URL {url} to {path}: {resp.status} {text}")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            data = resp.read(blocksize)
            while data:
                f.write(data)
                data = resp.read(blocksize)

    def uploadFile(self, url, path):
        with open(path, "rb") as f:
            resp = self.httpClient.request("PUT", url, data=f)
            text = resp.read().decode()
            if resp.status != 200:
                raise ARCHTTPError(resp.status, text, f"Upload {path} to {url} failed: {resp.status} {text}")

    def downloadListing(self, url):
        status, text = self._requestJSON("GET", url)
        if status != 200:
            raise ARCHTTPError(status, text, f"Error downloading listing {url}: {status} {text}")

        # /rest/1.0 compatibility
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                return {}
            else:
                raise

    def getDelegationsList(self):
        status, text = self._requestJSON("GET", f"{self.apiPath}/delegations")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting delegations list: {status} {text}")

        # /rest/1.0 compatibility
        try:
            return json.loads(text)["delegation"]
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                return []
            else:
                raise

    # Returns a tuple of CSR and delegation ID
    def requestNewDelegation(self):
        url = f"{self.apiPath}/delegations?action=new"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot get delegation CSR: {resp.status} {respstr}")
        return respstr, resp.getheader("Location").split("/")[-1]

    def uploadDelegation(self, delegationID, signedCert):
        url = f"{self.apiPath}/delegations/{delegationID}"
        headers = {"Content-Type": "application/x-pem-file"}
        resp = self.httpClient.request("PUT", url, data=signedCert, headers=headers)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"Cannot upload delegated cert for delegation {delegationID}: {resp.status} {respstr}")

    def getDelegationCert(self, delegationID):
        url = f"{self.apiPath}/delegations/{delegationID}?action=get"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"Cannot get cert for delegation {delegationID}: {resp.status} {respstr}")
        return respstr

    # returns CSR
    def requestDelegationRenewal(self, delegationID):
        url = f"{self.apiPath}/delegations/{delegationID}?action=renew"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr, f"Cannot renew delegation {delegationID}: {resp.status} {respstr}")
        return respstr

    def deleteDelegation(self, delegationID):
        url = f"{self.apiPath}/delegations/{delegationID}?action=delete"
        resp = self.httpClient.request("POST", url)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr, f"Cannot delete delegation {delegationID}: {resp.status} {respstr}")

    ### Higher level job operations ###

    # TODO: HARDCODED
    def uploadJobFiles(self, jobs, workers=10, blocksize=None, timeout=None):
        # create upload queue
        uploadQueue = queue.Queue()
        for job in jobs:
            self._getInputTransfers(job, uploadQueue)
            if job.errors:
                self.logger.debug(f"Skipping job {job.id} due to input file errors")
                continue
        if uploadQueue.empty():
            self.logger.debug("No local inputs to upload")
            return
        numWorkers = min(uploadQueue.qsize(), workers)

        # create REST clients for workers
        restClients = []
        for i in range(numWorkers):
            restClients.append(self.getClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                proxypath=self.httpClient.proxypath,
                logger=self.logger,
                blocksize=blocksize,
                timeout=timeout,
                apiBase=self.apiBase,
                version=self.version,
            ))
        self.logger.debug(f"Created {len(restClients)} upload workers")

        # run upload threads on upload queue
        with concurrent.futures.ThreadPoolExecutor(max_workers=numWorkers) as pool:
            futures = []
            for restClient in restClients:
                futures.append(pool.submit(
                    self._uploadTransferWorker,
                    restClient,
                    uploadQueue,
                    logger=self.logger,
                ))
            concurrent.futures.wait(futures)

        # close HTTP clients
        for restClient in restClients:
            restClient.close()

    # TODO: HARDCODED
    def downloadJobFiles(self, downloadDir, jobs, workers=10, blocksize=None, timeout=None):
        transferQueue = TransferQueue(workers)

        for job in jobs:
            cancelEvent = threading.Event()

            # Add diagnose files to transfer queue and remove them from
            # downloadfiles string. Replace download files with a list of
            # remaining download patterns.
            self._getDiagnoseTransfers(job, transferQueue, cancelEvent)

            # add job session directory as a listing transfer
            url = f"{self.apiPath}/jobs/{job.id}/session"
            transferQueue.put(FileTransfer(job, url, "", cancelEvent=cancelEvent, type="listing"))

        # create REST clients for workers
        restClients = []
        for i in range(workers):
            restClients.append(self.getClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                proxypath=self.httpClient.proxypath,
                logger=self.logger,
                blocksize=blocksize,
                timeout=timeout,
                apiBase=self.apiBase,
                version=self.version,
            ))

        self.logger.debug(f"Created {len(restClients)} download workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for restClient in restClients:
                futures.append(pool.submit(
                    self._downloadTransferWorker,
                    restClient,
                    transferQueue,
                    downloadDir,
                    logger=self.logger,
                ))
            concurrent.futures.wait(futures)

        for restClient in restClients:
            restClient.close()

    def createDelegation(self, lifetime=None):
        csr, delegationID = self.requestNewDelegation()
        pem = self._signCSR(csr, lifetime=lifetime)
        self.uploadDelegation(delegationID, pem)
        return delegationID

    def renewDelegation(self, delegationID, lifetime=None):
        csr = self.requestDelegationRenewal(delegationID)
        pem = self._signCSR(csr, lifetime=lifetime)
        try:
            self.uploadDelegation(delegationID, pem)
        except Exception as exc:
            self.deleteDelegation(delegationID)
            raise

    def submitJobs(self, queue, jobs, uploadData=True, workers=10, blocksize=None, timeout=None):
        # get delegation for proxy
        delegationID = self.createDelegation()

        # submit job descriptions to ARC CE
        self.createJobs(jobs, delegationID, queue)

        # upload jobs' local input data
        if uploadData:
            toupload = []
            for job in jobs:
                if not job.errors:
                    toupload.append(job)
            self.uploadJobFiles(toupload, workers=workers, blocksize=blocksize, timeout=timeout)

    ### Private support methods

    def _signCSR(self, csrStr, lifetime=None):
        with open(self.httpClient.proxypath) as f:
            proxyStr = f.read()
        proxyCert, _, issuerChains = parsePEM(proxyStr)
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
        csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
        cert = signRequest(csr, self.httpClient.proxypath, lifetime=lifetime).decode()
        pem = (cert + chain).encode()
        return pem

    def _getInputTransfers(self, job, uploadQueue):
        cancelEvent = threading.Event()
        for name, source in job.inputFiles.items():
            try:
                path = isLocalInputFile(name, source)
            except InputFileError as exc:
                job.errors.append(exc)
                self.logger.debug(f"Error parsing input {name} at {source} for job {job.id}: {exc}")
                continue
            if not path:
                self.logger.debug(f"Skipping non local input {name} at {source} for job {job.id}")
                continue

            if not os.path.isfile(path):
                msg = f"Local input {name} at {path} for job {job.id} is not a file"
                job.errors.append(InputFileError(msg))
                self.logger.debug(msg)
                continue

            url = f"{self.apiPath}/jobs/{job.id}/session/{name}"
            uploadQueue.put(FileTransfer(job, url, path, cancelEvent=cancelEvent))
            self.logger.debug(f"Will upload local input {name} at {path} for job {job.id}")

    def _getDiagnoseTransfers(self, job, transferQueue, cancelEvent):
        DIAG_FILES = [
            "failed", "local", "errors", "description", "diag", "comment",
            "status", "acl", "xml", "input", "output", "input_status",
            "output_status", "statistics"
        ]

        if not job.downloadFiles:
            self.logger.debug(f"No files to download for job {job.id}")

        # add all diagnose files to transfer queue and create
        # a list of download patterns
        newDownloads = []
        diagFiles = set()  # to remove any possible duplications
        for download in job.downloadFiles:
            if download.startswith("diagnose="):
                # remove diagnose= part
                diagnose = download[len("diagnose="):]
                if not diagnose:
                    self.logger.debug(f"Skipping empty download entry: {download}")
                    continue  # error?

                # add all files if entire log folder is specified
                if diagnose.endswith("/"):
                    self.logger.debug(f"Will download all diagnose files to {diagnose}")
                    for diagFile in DIAG_FILES:
                        diagFiles.add(f"{diagnose}{diagFile}")

                else:
                    diagFile = diagnose.split("/")[-1]
                    if diagFile not in DIAG_FILES:
                        self.logger.debug(f"Skipping download {download} because of unknown diagnose file {diagFile}")
                        continue  # error?
                    self.logger.debug(f"Will download diagnose file {diagFile} to {download}")
                    diagFiles.add(diagnose)
            else:
                self.logger.debug(f"Will download {download}")
                newDownloads.append(download)

        for diagFile in diagFiles:
            diagName = diagFile.split("/")[-1]
            url = f"{self.apiPath}/jobs/{job.id}/diagnose/{diagName}"
            transferQueue.put(FileTransfer(job, url, diagFile, cancelEvent=cancelEvent, type="diagnose"))
        job.downloadFiles = newDownloads

    def _requestJSON(self, *args, **kwargs):
        return self._requestJSONStatic(self.httpClient, *args, **kwargs)

    def _manageJobs(self, jobs, action):
        if not jobs:
            return []

        # JSON data for request
        tomanage = [{"id": job.id} for job in jobs]

        # /rest/1.0 compatibility
        if len(tomanage) == 1:
            jsonData = {"job": tomanage[0]}
        else:
            jsonData = {"job": tomanage}

        # execute action and get JSON result
        url = f"{self.apiPath}/jobs?action={action}"
        status, text = self._requestJSON("POST", url, jsonData=jsonData)
        if status != 201:
            raise ARCHTTPError(status, text, f"ARC jobs \"{action}\" action error: {status} {text}")
        jsonData = json.loads(text)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            return [jsonData["job"]]
        else:
            return jsonData["job"]

    def _createTransfersFromListing(self, job, listing, path, cancelEvent, transferQueue):
        if "file" in listing:
            # /rest/1.0 compatibility
            if not isinstance(listing["file"], list):
                listing["file"] = [listing["file"]]

            for f in listing["file"]:
                if path:
                    newpath = f"{path}/{f}"
                else:  # if session root, slash needs to be skipped
                    newpath = f
                if not self._filterOutFile(job.downloadFiles, newpath):
                    url = f"{self.apiPath}/jobs/{job.id}/session/{newpath}"
                    transferQueue.put(FileTransfer(job, url, newpath, cancelEvent, type="file"))
        if "dir" in listing:
            # /rest/1.0 compatibility
            if not isinstance(listing["dir"], list):
                listing["dir"] = [listing["dir"]]

            for d in listing["dir"]:
                if path:
                    newpath = f"{path}/{d}"
                else:  # if session root, slash needs to be skipped
                    newpath = d
                if not self._filterOutListing(job.downloadFiles, newpath):
                    url = f"{self.apiPath}/jobs/{job.id}/session/{newpath}"
                    transferQueue.put(FileTransfer(job, url, newpath, cancelEvent, type="listing"))

    def _filterOutFile(self, downloadFiles, filePath):
        if not downloadFiles:
            return False
        for pattern in downloadFiles:
            # direct match
            if pattern == filePath:
                return False
            # recursive folder match
            elif pattern.endswith("/") and filePath.startswith(pattern):
                return False
            # entire session directory, not matched by above if
            elif pattern == "/":
                return False
        return True

    def _filterOutListing(self, downloadFiles, listingPath):
        if not downloadFiles:
            return False
        for pattern in downloadFiles:
            # part of pattern
            if pattern.startswith(listingPath):
                return False
            # recursive folder match
            elif pattern.endswith("/") and listingPath.startswith(pattern):
                return False
        return True

    def _findQueue(self, queue, ceInfo):
        compShares = ceInfo.get("Domains", {}) \
                           .get("AdminDomain", {}) \
                           .get("Services", {}) \
                           .get("ComputingService", {}) \
                           .get("ComputingShare", [])
        if not compShares:
            raise ARCError("No queues found on cluster")

        # /rest/1.0 compatibility
        if isinstance(compShares, dict):
            compShares = [compShares]

        for compShare in compShares:
            if compShare.get("Name", None) == queue:
                # Queues are defined as ComputingShares. There are some shares
                # that are mapped to another share. Such a share is never a
                # queue externally. So if the name of the such share is used as
                # a queue, the result has to be empty.
                if "MappingPolicy" in compShare:
                    return None
                else:
                    return compShare
        return None

    def _findRuntimes(self, ceInfo):
        appenvs = ceInfo.get("Domains", {}) \
                        .get("AdminDomain", {}) \
                        .get("Services", {}) \
                        .get("ComputingService", {}) \
                        .get("ComputingManager", {}) \
                        .get("ApplicationEnvironments", {}) \
                        .get("ApplicationEnvironment", [])

        # /rest/1.0 compatibility
        if isinstance(appenvs, dict):
            appenvs = [appenvs]

        runtimes = []
        for env in appenvs:
            if "AppName" in env:
                envname = env["AppName"]
                if "AppVersion" in env:
                    envname += f"-{env['AppVersion']}"
                runtimes.append(envname)
        return runtimes

    @classmethod
    def _requestJSONStatic(cls, httpClient, *args, headers={}, **kwargs):
        headers["Accept"] = "application/json"
        resp = httpClient.request(*args, headers=headers, **kwargs)
        text = resp.read().decode()
        return resp.status, text

    @classmethod
    def _uploadTransferWorker(cls, restClient, uploadQueue, logger=getNullLogger()):
        while True:
            try:
                upload = uploadQueue.get(block=False)
            except queue.Empty:
                break
            uploadQueue.task_done()

            job = upload.job
            if upload.cancelEvent.is_set():
                logger.debug(f"Skipping upload for cancelled job {job.id}")
                continue

            try:
                restClient.uploadFile(upload.url, upload.path)
            except Exception as exc:
                upload.cancelEvent.set()
                job.errors.append(exc)
                if isinstance(exc, ARCHTTPError):
                    logger.debug(str(exc))
                else:
                    logger.debug(f"Upload {upload.path} to {upload.url} for job {job.id} failed: {exc}")

    @classmethod
    def _downloadTransferWorker(cls, restClient, transferQueue, downloadDir, logger=getNullLogger()):
        while True:
            try:
                transfer = transferQueue.get()
            except TransferQueueEmpty:
                break

            job = transfer.job
            if transfer.cancelEvent.is_set():
                logger.debug(f"Skipping download for cancelled job {job.id}")
                continue

            try:
                if transfer.type in ("file", "diagnose"):
                    # download file
                    path = f"{downloadDir}/{job.id}/{transfer.path}"
                    # TODO: Python >= 3.7
                    #blocksize = restClient.httpClient.conn.blocksize
                    blocksize = None
                    try:
                        restClient.downloadFile(transfer.url, path, blocksize=blocksize)
                    except Exception as exc:
                        error = exc
                        if isinstance(exc, ARCHTTPError):
                            if exc.status == 404:
                                if transfer.type == "diagnose":
                                    error = MissingDiagnoseFile(transfer.url)
                                else:
                                    error = MissingOutputFile(transfer.url)
                        else:
                            transfer.cancelEvent.set()
                        job.errors.append(error)

                        logger.debug(f"Download {transfer.url} to {path} for job {job.id} failed: {error}")

                    else:
                        logger.debug(f"Download {transfer.url} to {path} for job {job.id} successful")

                elif transfer.type == "listing":
                    # download listing
                    try:
                        listing = restClient.downloadListing(transfer.url)
                    except ARCHTTPError as exc:
                        # work around for invalid output in ARC 6
                        if exc.text == "":
                            listing = {}
                        else:
                            raise
                    except Exception as exc:
                        if not isinstance(exc, ARCHTTPError):
                            transfer.cancelEvent.set()
                        job.errors.append(exc)
                        logger.debug(f"Download listing {transfer.url} for job {job.id} failed: {exc}")
                    else:
                        # create new transfer jobs
                        restClient._createTransfersFromListing(
                            job, listing, transfer.path, transfer.cancelEvent, transferQueue
                        )
                        logger.debug(f"Download listing {transfer.url} for job {job.id} successful")

            # every possible exception needs to be handled, otherwise the
            # threads will lock up
            except:
                import traceback
                excstr = traceback.format_exc()
                transfer.cancelEvent.set()
                job.errors.append(Exception(excstr))
                logger.debug(f"Download URL {transfer.url} and path {transfer.path} for job {job.id} failed: {excstr}")

    ### public static methods ###

    @classmethod
    def getAPIVersionsStatic(cls, httpClient, apiBase="/arex"):
        status, text = cls._requestJSONStatic(httpClient, "GET", f"{apiBase}/rest")
        if status != 200:
            raise ARCHTTPError(status, text, f"Error getting ARC API versions: {status} {text}")
        apiVersions = json.loads(text)

        # /rest/1.0 compatibility
        if not isinstance(apiVersions["version"], list):
            return [apiVersions["version"]]
        else:
            return apiVersions["version"]

    @classmethod
    def getClient(cls, url=None, host=None, port=None, proxypath=None, logger=getNullLogger(), blocksize=None, timeout=None, version=None, apiBase="/arex"):
        IMPLEMENTED_VERSIONS = {
            "1.0" : ARCRest_1_0,
            "1.1" : ARCRest_1_1,
        }

        httpClient = HTTPClient(url=url, host=host, port=port, proxypath=proxypath, logger=logger, blocksize=blocksize, timeout=timeout)
        apiVersions = cls.getAPIVersionsStatic(httpClient, apiBase=apiBase)
        if not apiVersions:
            raise ARCError("No supported API versions on CE")

        if version is not None:
            if version not in IMPLEMENTED_VERSIONS:
                raise ARCError(f"No client support for requested API version {version}")
            if version not in apiVersions:
                raise ARCError(f"API version {version} not among CE supported API versions {apiVersions}")
            apiVersion = version
        else:
            apiVersion = None
            for version in reversed(apiVersions):
                if version in IMPLEMENTED_VERSIONS:
                    apiVersion = version
                    break
            if apiVersion is None:
                raise ARCError(f"No client support for CE supported API versions: {apiVersions}")

        logger.debug(f"API version {apiVersion} selected")
        return IMPLEMENTED_VERSIONS[apiVersion](httpClient, apiBase=apiBase, logger=logger)


class ARCRest_1_0(ARCRest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = "1.0"
        self.apiPath = f"{self.apiBase}/rest/{self.version}"

    def createJobs(self, jobs, delegationID, queue):
        import arc
        ceInfo = self.getCEInfo()
        queueInfo = self._findQueue(queue, ceInfo)
        if queueInfo is None:
            raise MatchmakingError(f"Requested queue {queue} does not exist")
        runtimes = self._findRuntimes(ceInfo)

        jobdescs = arc.JobDescriptionList()
        tosubmit = []  # sublist of jobs that will be submitted
        bulkdesc = ""
        for job in jobs:
            # parse job description
            if not arc.JobDescription_Parse(job.descstr, jobdescs):
                job.errors.append(DescriptionParseError("Failed to parse description"))
                self.logger.debug(f"Failed to parse description {job.descstr}")
                continue
            desc = jobdescs[-1]

            # matchmaking
            #
            # TODO: only direct version comparison is done for runtime
            # envrionment; some other parameters are not matched:
            # - memory (xrsl): which value is it from info document?
            #   MaxVirtualMemory?
            # - disk: no relevant value found in info document
            wallTime = desc.Resources.TotalWallTime.range.max
            envs = [str(env) for env in desc.Resources.RunTimeEnvironment.getSoftwareList()]
            maxWallTime = int(queueInfo.get("MaxWallTime", sys.maxsize))
            error = None
            if wallTime > maxWallTime:
                error = MatchmakingError(f"Requested wall time {wallTime} higher than available {maxWallTime}")
            else:
                for env in envs:
                    if env not in runtimes:
                        error = MatchmakingError(f"Requested environment {env} not available")
                        break
            if error is not None:
                job.errors.append(error)
                self.logger.debug(str(error))
                continue

            # add queue and delegation to job description, do client processing
            # of job description
            desc.Resources.QueueName = queue
            desc.DataStaging.DelegationID = delegationID
            processJobDescription(desc)

            # get input files from description
            job.getArclibInputFiles(desc)

            # read name from description
            job.name = desc.Identification.JobName

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = desc.UnParse("emies:adl")
            if not unparseResult[0]:
                job.errors.append(DescriptionUnparseError(f"Could not unparse modified description of job {job.name}"))
                self.logger.debug(f"Could not unparse modified description of job {job.name}")
                continue
            descstart = unparseResult[1].find("<ActivityDescription")
            bulkdesc += unparseResult[1][descstart:]

            tosubmit.append(job)

        if not tosubmit:
            return

        # merge into bulk description
        if len(tosubmit) > 1:
            bulkdesc = f"<ActivityDescriptions>{bulkdesc}</ActivityDescriptions>"

        # submit jobs to ARC
        status, text = self._requestJSON(
            "POST",
            f"{self.apiPath}/jobs?action=new",
            data=bulkdesc,
            headers={"Content-Type": "application/xml"},
        )
        if status != 201:
            raise ARCHTTPError(status, text, f"Error submitting jobs: {status} {text}")
        jsonData = json.loads(text)

        # /rest/1.0 compatibility
        if isinstance(jsonData["job"], dict):
            results = [jsonData["job"]]
        else:
            results = jsonData["job"]

        for job, result in zip(tosubmit, results):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 201:
                job.errors.append(ARCHTTPError(code, reason, f"Error submitting job {job.descstr}: {reason}"))
            else:
                job.id = result["id"]
                job.state = result["state"]


class ARCRest_1_1(ARCRest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = "1.1"
        self.apiPath = f"{self.apiBase}/rest/{self.version}"

    def createJobs(self, jobs, delegationID, queue):
        import arc
        ceInfo = self.getCEInfo()
        queueInfo = self._findQueue(queue, ceInfo)
        if queueInfo is None:
            raise MatchmakingError(f"Requested queue {queue} does not exist")
        runtimes = self._findRuntimes(ceInfo)

        jobdescs = arc.JobDescriptionList()
        tosubmit = []  # sublist of jobs that will be submitted
        bulkdesc = ""
        for job in jobs:
            # parse job description
            if not arc.JobDescription_Parse(job.descstr, jobdescs):
                job.errors.append(DescriptionParseError("Failed to parse description"))
                self.logger.debug(f"Failed to parse description {job.descstr}")
                continue
            desc = jobdescs[-1]

            # matchmaking
            #
            # TODO: only direct version comparison is done for runtime
            # envrionment; some other parameters are not matched:
            # - memory (xrsl): which value is it from info document?
            #   MaxVirtualMemory?
            # - disk: no relevant value found in info document
            wallTime = desc.Resources.TotalWallTime.range.max
            envs = [str(env) for env in desc.Resources.RunTimeEnvironment.getSoftwareList()]
            maxWallTime = int(queueInfo.get("MaxWallTime", sys.maxsize))
            error = None
            if wallTime > maxWallTime:
                error = MatchmakingError(f"Requested wall time {wallTime} higher than available {maxWallTime}")
            else:
                for env in envs:
                    if env not in runtimes:
                        error = MatchmakingError(f"Requested environment {env} not available")
                        break
            if error is not None:
                job.errors.append(error)
                self.logger.debug(str(error))
                continue

            # do client processing of job description
            processJobDescription(desc)

            # get input files from description
            job.getArclibInputFiles(desc)

            # read name from description
            job.name = desc.Identification.JobName

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = desc.UnParse("emies:adl")
            if not unparseResult[0]:
                job.errors.append(DescriptionUnparseError(f"Could not unparse modified description of job {job.name}"))
                self.logger.debug(f"Could not unparse modified description of job {job.name}")
                continue
            descstart = unparseResult[1].find("<ActivityDescription")
            bulkdesc += unparseResult[1][descstart:]

            tosubmit.append(job)

        if not tosubmit:
            return

        # merge into bulk description
        if len(tosubmit) > 1:
            bulkdesc = f"<ActivityDescriptions>{bulkdesc}</ActivityDescriptions>"

        # submit jobs to ARC
        status, text = self._requestJSON(
            "POST",
            f"{self.apiPath}/jobs?action=new&queue={queue}&delegation_id={delegationID}",
            data=bulkdesc,
            headers={"Content-Type": "application/xml"},
        )
        if status != 201:
            raise ARCHTTPError(status, text, f"Error submitting jobs: {status} {text}")
        jsonData = json.loads(text)

        for job, result in zip(tosubmit, jsonData["job"]):
            code, reason = int(result["status-code"]), result["reason"]
            if code != 201:
                job.errors.append(ARCHTTPError(code, reason, f"Error submitting job {job.descstr}: {reason}"))
            else:
                job.id = result["id"]
                job.state = result["state"]


class FileTransfer:

    def __init__(self, job, url, path, cancelEvent=None, type=None):
        self.job = job
        self.url = url
        self.path = path
        self.cancelEvent = cancelEvent
        if self.cancelEvent is None:
            self.cancelEvent = threading.Event()
        self.type = type


class ARCJob:

    def __init__(self, id=None, descstr=None):
        self.id = id
        self.descstr = descstr
        self.name = None
        self.delegations = []
        self.state = None
        self.errors = []
        self.downloadFiles = []
        self.inputFiles = {}
        self.restartState = None

        self.ExecutionNode = None
        self.UsedTotalWallTime = None
        self.UsedTotalCPUTime = None
        self.RequestedTotalWallTime = None
        self.RequestedTotalCPUTime = None
        self.RequestedSlots = None
        self.ExitCode = None
        self.Type = None
        self.LocalIDFromManager = None
        self.WaitingPosition = None
        self.Owner = None
        self.LocalOwner = None
        self.StdIn = None
        self.StdOut = None
        self.StdErr = None
        self.LogDir = None
        self.Queue = None
        self.UsedMainMemory = None
        self.SubmissionTime = None
        self.EndTime = None
        self.WorkingAreaEraseTime = None
        self.ProxyExpirationTime = None
        self.Error = []

    def updateFromInfo(self, infoDocument):
        infoDict = infoDocument.get("ComputingActivity", {})
        if not infoDict:
            return

        if "Name" in infoDict:
            self.name = infoDict["Name"]

        # get state from a list of activity states in different systems
        for state in infoDict.get("State", []):
            if state.startswith("arcrest:"):
                self.state = state[len("arcrest:"):]

        if "Error" in infoDict:
            # /rest/1.0 compatibility
            if isinstance(infoDict["Error"], list):
                self.Error = infoDict["Error"]
            else:
                self.Error = [infoDict["Error"]]

        if "ExecutionNode" in infoDict:
            # /rest/1.0 compatibility
            if isinstance(infoDict["ExecutionNode"], list):
                self.ExecutionNode = infoDict["ExecutionNode"]
            else:
                self.ExecutionNode = [infoDict["ExecutionNode"]]
            # throw out all non ASCII characters from nodes
            for i in range(len(self.ExecutionNode)):
                self.ExecutionNode[i] = ''.join([i for i in self.ExecutionNode[i] if ord(i) < 128])

        if "UsedTotalWallTime" in infoDict:
            self.UsedTotalWallTime = int(infoDict["UsedTotalWallTime"])

        if "UsedTotalCPUTime" in infoDict:
            self.UsedTotalCPUTime = int(infoDict["UsedTotalCPUTime"])

        if "RequestedTotalWallTime" in infoDict:
            self.RequestedTotalWallTime = int(infoDict["RequestedTotalWallTime"])

        if "RequestedTotalCPUTime" in infoDict:
            self.RequestedTotalCPUTime = int(infoDict["RequestedTotalCPUTime"])

        if "RequestedSlots" in infoDict:
            self.RequestedSlots = int(infoDict["RequestedSlots"])

        if "ExitCode" in infoDict:
            self.ExitCode = int(infoDict["ExitCode"])

        if "Type" in infoDict:
            self.Type = infoDict["Type"]

        if "LocalIDFromManager" in infoDict:
            self.LocalIDFromManager = infoDict["LocalIDFromManager"]

        if "WaitingPosition" in infoDict:
            self.WaitingPosition = int(infoDict["WaitingPosition"])

        if "Owner" in infoDict:
            self.Owner = infoDict["Owner"]

        if "LocalOwner" in infoDict:
            self.LocalOwner = infoDict["LocalOwner"]

        if "StdIn" in infoDict:
            self.StdIn = infoDict["StdIn"]

        if "StdOut" in infoDict:
            self.StdOut = infoDict["StdOut"]

        if "StdErr" in infoDict:
            self.StdErr = infoDict["StdErr"]

        if "LogDir" in infoDict:
            self.LogDir = infoDict["LogDir"]

        if "Queue" in infoDict:
            self.Queue = infoDict["Queue"]

        if "UsedMainMemory" in infoDict:
            self.UsedMainMemory = int(infoDict["UsedMainMemory"])

        if "SubmissionTime" in infoDict:
            self.SubmissionTime = datetime.datetime.strptime(
                infoDict["SubmissionTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "EndTime" in infoDict:
            self.EndTime = datetime.datetime.strptime(
                infoDict["EndTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "WorkingAreaEraseTime" in infoDict:
            self.WorkingAreaEraseTime = datetime.datetime.strptime(
                infoDict["WorkingAreaEraseTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if "ProxyExpirationTime" in infoDict:
            self.ProxyExpirationTime = datetime.datetime.strptime(
                infoDict["ProxyExpirationTime"],
                "%Y-%m-%dT%H:%M:%SZ"
            )

        restartStates = infoDict.get("RestartState", [])
        # /rest/1.0 compatibility
        if not isinstance(restartStates, list):
            restartStates = [restartStates]
        # parse out the arcrest restart state
        for state in restartStates:
            if state.startswith("arcrest:"):
                self.restartState = state[len("arcrest:"):]

    def getArclibInputFiles(self, desc):
        self.inputFiles = {}
        for infile in desc.DataStaging.InputFiles:
            source = None
            if len(infile.Sources) > 0:
                source = infile.Sources[0].fullstr()
            self.inputFiles[infile.Name] = source


class TransferQueue:

    def __init__(self, numWorkers):
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.barrier = threading.Barrier(numWorkers)

    def put(self, val):
        with self.lock:
            self.queue.put(val)
            self.barrier.reset()

    def get(self):
        while True:
            with self.lock:
                if not self.queue.empty():
                    val = self.queue.get()
                    self.queue.task_done()
                    return val

            try:
                self.barrier.wait()
            except threading.BrokenBarrierError:
                continue
            else:
                raise TransferQueueEmpty()


def isLocalInputFile(name, path):
    """
    Return path if local or empty string if remote URL.

    Raises:
        - InputFileError
    """
    if not path:
        return name

    try:
        url = urlparse(path)
    except ValueError as exc:
        raise InputFileError(f"Error parsing source {path} of file {name}: {exc}")
    if url.scheme not in ("file", None, "") or url.hostname:
        return ""

    return url.path


def processJobDescription(jobdesc):
    import arc
    exepath = jobdesc.Application.Executable.Path
    if exepath and exepath.startswith("/"):  # absolute paths are on compute nodes
        exepath = ""
    inpath = jobdesc.Application.Input
    outpath = jobdesc.Application.Output
    errpath = jobdesc.Application.Error
    logpath = jobdesc.Application.LogDir

    exePresent = False
    stdinPresent = False
    for infile in jobdesc.DataStaging.InputFiles:
        if exepath == infile.Name:
            exePresent = True
        elif inpath == infile.Name:
            stdinPresent = True

    stdoutPresent = False
    stderrPresent = False
    logPresent = False
    for outfile in jobdesc.DataStaging.OutputFiles:
        if outpath == outfile.Name:
            stdoutPresent = True
        elif errpath == outfile.Name:
            stderrPresent = True
        elif logpath == outfile.Name or logpath == outfile.Name[:-1]:
            logPresent = True

    if exepath and not exePresent:
        infile = arc.InputFileType()
        infile.Name = exepath
        jobdesc.DataStaging.InputFiles.append(infile)

    if inpath and not stdinPresent:
        infile = arc.InputFileType()
        infile.Name = inpath
        jobdesc.DataStaging.InputFiles.append(infile)

    if outpath and not stdoutPresent:
        outfile = arc.OutputFileType()
        outfile.Name = outpath
        jobdesc.DataStaging.OutputFiles.append(outfile)

    if errpath and not stderrPresent:
        outfile = arc.OutputFileType()
        outfile.Name = errpath
        jobdesc.DataStaging.OutputFiles.append(outfile)

    if logpath and not logPresent:
        outfile = arc.OutputFileType()
        if not logpath.endswith('/'):
            outfile.Name = f'{logpath}/'
        else:
            outfile.Name = logpath
        jobdesc.DataStaging.OutputFiles.append(outfile)


class TransferQueueEmpty(Exception):
    pass
