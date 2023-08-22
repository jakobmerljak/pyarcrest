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
import threading
from urllib.parse import urlparse

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from pyarcrest.common import getNullLogger
from pyarcrest.errors import (ARCError, ARCHTTPError, DescriptionParseError,
                              DescriptionUnparseError, InputFileError,
                              InputUploadError, MatchmakingError,
                              MissingDiagnoseFile, MissingOutputFile,
                              NoValueInARCResult)
from pyarcrest.http import HTTPClient
from pyarcrest.x509 import parsePEM, signRequest

# TODO: blocksize can only be used with Python >= 3.7


class ARCRest:

    DIAGNOSE_FILES = [
        "failed", "local", "errors", "description", "diag", "comment",
        "status", "acl", "xml", "input", "output", "input_status",
        "output_status", "statistics"
    ]

    def __init__(self, httpClient, token=None, proxypath=None, apiBase="/arex", log=getNullLogger()):
        """
        Initialize the base object.

        Note that this class should not be instantiated directly because
        additional implementations of attributes and methods are required from
        derived classes.
        """
        if not token and not proxypath:
            raise ARCError("One of either token or proxy path is required for authentication")
        self.httpClient = httpClient
        self.token = token
        self.proxypath = proxypath if not token else None
        self.apiBase = apiBase
        self.log = log

    def close(self):
        self.httpClient.close()

    ### Direct operations on ARC CE ###

    def getAPIVersions(self):
        return self.getAPIVersionsStatic(self.httpClient, self.apiBase, self.token)

    def getCEInfo(self):
        status, text = self._requestJSON("GET", "/info")
        if status != 200:
            raise ARCHTTPError(status, text)
        return json.loads(text)

    def getJobsList(self):
        status, text = self._requestJSON("GET", "/jobs")
        if status != 200:
            raise ARCHTTPError(status, text)

        try:
            jsonData = json.loads(text)["job"]
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                jsonData = []
            else:
                raise
        # /rest/1.0 compatibility
        if not isinstance(jsonData, list):
            jsonData = [jsonData]

        return [job["id"] for job in jsonData]

    def createJobs(self, description, queue=None, delegationID=None, isADL=True):
        raise Exception("Not implemented in the base class")

    def getJobsInfo(self, jobs):
        responses = self._manageJobs(jobs, "info")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 200:
                results.append(ARCHTTPError(code, reason))
            elif "info_document" not in response:
                results.append(NoValueInARCResult(f"No info document in successful info response for job {job}"))
            else:
                results.append(self._parseJobInfo(response["info_document"]))
        return results

    def getJobsStatus(self, jobs):
        responses = self._manageJobs(jobs, "status")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 200:
                results.append(ARCHTTPError(code, reason))
            elif "state" not in response:
                results.append(NoValueInARCResult("No state in successful status response"))
            else:
                results.append(response["state"])
        return results

    def killJobs(self, jobs):
        responses = self._manageJobs(jobs, "kill")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 202:
                results.append(ARCHTTPError(code, reason))
            else:
                results.append(True)
        return results

    def cleanJobs(self, jobs):
        responses = self._manageJobs(jobs, "clean")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 202:
                results.append(ARCHTTPError(code, reason))
            else:
                results.append(True)
        return results

    def restartJobs(self, jobs):
        responses = self._manageJobs(jobs, "restart")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 202:
                results.append(ARCHTTPError(code, reason))
            else:
                results.append(True)
        return results

    def getJobsDelegations(self, jobs):
        responses = self._manageJobs(jobs, "delegations")
        results = []
        for job, response in zip(jobs, responses):
            code, reason = int(response["status-code"]), response["reason"]
            if code != 200:
                results.append(ARCHTTPError(code, reason))
            elif "delegation_id" not in response:
                results.append(NoValueInARCResult("No delegation ID in successful response"))
            else:
                # /rest/1.0 compatibility
                if isinstance(response["delegation_id"], list):
                    results.append(response["delegation_id"])
                else:
                    results.append([response["delegation_id"]])
        return results

    def downloadFile(self, jobid, sessionPath, filePath):
        self._downloadURL(f"/jobs/{jobid}/session/{sessionPath}", filePath)

    def uploadFile(self, jobid, sessionPath, filePath):
        urlPath = f"/jobs/{jobid}/session/{sessionPath}"
        with open(filePath, "rb") as f:
            resp = self._request("PUT", urlPath, data=f)
            text = resp.read().decode()
            if resp.status != 200:
                raise ARCHTTPError(resp.status, text)

    def downloadListing(self, jobid, sessionPath):
        urlPath = f"/jobs/{jobid}/session/{sessionPath}"
        status, text = self._requestJSON("GET", urlPath)
        if status != 200:
            raise ARCHTTPError(status, text)

        # /rest/1.0 compatibility
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                return {}
            else:
                raise

    def downloadDiagnoseFile(self, jobid, name, path):
        if name not in self.DIAGNOSE_FILES:
            raise ARCError(f"Invalid control dir file requested: {name}")
        urlPath = f"/jobs/{jobid}/diagnose/{name}"
        self._downloadURL(urlPath, path)

    def getDelegationsList(self, type=None):
        params = {}
        if type:
            if type not in ("x509", "jwt"):
                raise ARCError(f"Invalid type parameter: {type}")
            else:
                params["type"] = type
        status, text = self._requestJSON("GET", "/delegations", params=params)
        if status != 200:
            raise ARCHTTPError(status, text)

        # /rest/1.0 compatibility
        try:
            return json.loads(text)["delegation"]
        except json.JSONDecodeError as exc:
            if exc.doc == "":
                return []
            else:
                raise

    # TODO: document: token parameter and attribute take priority while proxy
    #       is the default
    #
    # Token functionality is implemented in the base class as both auth methods
    # will be supported in the future. The specific implementation of 1.0
    # version will override the behavior. This is probably better than
    # 1.1 being in specific class.
    #
    # token parameter allows delegation creation from non-attribute tokens.
    #
    # Returns a tuple of delegation ID and CSR (if using cert delegations,
    # otherwise the second value is None and can be ignored).
    def newDelegation(self):
        headers = {}
        params = {"action": "new"}
        if self.token:
            headers["X-Delegation"] = f"Bearer {self.token}"
            params["type"] = "jwt"
        resp = self._request("POST", "/delegations", headers=headers, params=params)
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr)
        if self.token:
            respstr = None
        return resp.getheader("Location").split("/")[-1], respstr

    def uploadCertDelegation(self, delegationID, cert):
        url = f"/delegations/{delegationID}"
        headers = {"Content-Type": "application/x-pem-file"}
        resp = self._request("PUT", url, data=cert, headers=headers)
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr)

    def getDelegation(self, delegationID):
        url = f"/delegations/{delegationID}"
        resp = self._request("POST", url, params={"action": "get"})
        respstr = resp.read().decode()
        if resp.status != 200:
            raise ARCHTTPError(resp.status, respstr)
        return respstr

    # returns CSR if proxy cert is used, None otherwise
    def renewDelegation(self, delegationID):
        headers = {}
        params = {"action": "renew"}
        if self.token:
            headers["X-Delegation"] = f"Bearer {self.token}"
        url = f"/delegations/{delegationID}"
        resp = self._request("POST", url, headers=headers, params=params)
        respstr = resp.read().decode()
        if resp.status != 201:
            raise ARCHTTPError(resp.status, respstr)
        if self.token:
            respstr = None
        return respstr

    def deleteDelegation(self, delegationID):
        url = f"/delegations/{delegationID}"
        resp = self._request("POST", url, params={"action": "delete"})
        respstr = resp.read().decode()
        if resp.status != 202:
            raise ARCHTTPError(resp.status, respstr)

    ### Higher level job operations ###

    def uploadJobFiles(self, jobids, jobInputs, workers=10, blocksize=None, timeout=None):
        resultDict = {jobid: [] for jobid in jobids}

        # create upload queue
        uploadQueue = queue.Queue()
        for jobid, inputFiles in zip(jobids, jobInputs):
            try:
                self._addInputTransfers(uploadQueue, jobid, inputFiles)
            except InputFileError as exc:
                resultDict[jobid].append(exc)
                self.log.debug(f"Skipping job {jobid} due to input file error: {exc}")

        if uploadQueue.empty():
            self.log.debug("No local inputs to upload")
            return [resultDict[jobid] for jobid in jobids]

        errorQueue = queue.Queue()

        # create REST clients for workers
        numWorkers = min(uploadQueue.qsize(), workers)
        restClients = []
        for i in range(numWorkers):
            restClients.append(self.getClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                blocksize=blocksize,
                timeout=timeout,
                token=self.token,
                proxypath=self.proxypath,
                apiBase=self.apiBase,
                log=self.log,
                version=self.version,
            ))
        self.log.debug(f"Created {len(restClients)} upload workers")

        # run upload threads on upload queue
        with concurrent.futures.ThreadPoolExecutor(max_workers=numWorkers) as pool:
            futures = []
            for restClient in restClients:
                futures.append(pool.submit(
                    self._uploadTransferWorker,
                    restClient,
                    uploadQueue,
                    errorQueue,
                    log=self.log,
                ))
            concurrent.futures.wait(futures)

        # close HTTP clients
        for restClient in restClients:
            restClient.close()

        # get transfer errors
        while not errorQueue.empty():
            error = errorQueue.get()
            resultDict[error["jobid"]].append(error["error"])
            errorQueue.task_done()

        return [resultDict[jobid] for jobid in jobids]

    def downloadJobFiles(self, downloadDir, jobids, outputFilters={}, diagnoseFiles={}, diagnoseDirs={}, workers=10, blocksize=None, timeout=None):
        resultDict = {jobid: [] for jobid in jobids}
        transferQueue = TransferQueue(workers)

        for jobid in jobids:
            cancelEvent = threading.Event()
            # add diagnose files to transfer queue
            try:
                self._addDiagnoseTransfers(transferQueue, jobid, downloadDir, diagnoseFiles, diagnoseDirs, cancelEvent)
            except ARCError as exc:
                resultDict[jobid].append(exc)
                continue
            # add job session directory as a listing transfer
            path = os.path.join(downloadDir, jobid)
            transferQueue.put(Transfer(jobid, "", path, type="listing", cancelEvent=cancelEvent))

        errorQueue = queue.Queue()

        # create REST clients for workers
        restClients = []
        for i in range(workers):
            restClients.append(self.getClient(
                host=self.httpClient.conn.host,
                port=self.httpClient.conn.port,
                blocksize=blocksize,
                timeout=timeout,
                token=self.token,
                proxypath=self.proxypath,
                apiBase=self.apiBase,
                log=self.log,
                version=self.version,
            ))

        self.log.debug(f"Created {len(restClients)} download workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for restClient in restClients:
                futures.append(pool.submit(
                    self._downloadTransferWorker,
                    restClient,
                    transferQueue,
                    errorQueue,
                    downloadDir,
                    outputFilters,
                    self.log,
                ))
            concurrent.futures.wait(futures)

        for restClient in restClients:
            restClient.close()

        # get transfer errors
        while not errorQueue.empty():
            error = errorQueue.get()
            resultDict[error["jobid"]].append(error["error"])
            errorQueue.task_done()

        return [resultDict[jobid] for jobid in jobids]

    def createDelegation(self, proxyLifetime=None):
        delegationID, csr = self.newDelegation()
        if not self.token:
            try:
                pem = self._signCSR(csr, proxyLifetime)
                self.uploadCertDelegation(delegationID, pem)
                return delegationID
            except Exception:
                self.deleteDelegation(delegationID)
                raise
        return delegationID

    def refreshDelegation(self, delegationID, proxyLifetime=None):
        csr = self.renewDelegation(delegationID)
        if not self.token:
            try:
                pem = self._signCSR(csr, proxyLifetime)
                self.uploadCertDelegation(delegationID, pem)
            except Exception:
                self.deleteDelegation(delegationID)
                raise

    def submitJobs(self, descs, queue, delegationID=None, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None):
        raise Exception("Not implemented in the base class")

    def matchJob(self, ceInfo, queue=None, runtimes=[], walltime=None):
        errors = []

        if queue:
            try:
                self._matchQueue(ceInfo, queue)
            except MatchmakingError as exc:
                errors.append(exc)

            # matching walltime requires queue
            if walltime:
                try:
                    self._matchWalltime(ceInfo, queue, walltime)
                except MatchmakingError as exc:
                    errors.append(exc)

        for runtime in runtimes:
            try:
                self._matchRuntime(ceInfo, runtime)
            except MatchmakingError as exc:
                errors.append(exc)

        return errors

    ### Static operations ###

    @classmethod
    def getAPIVersionsStatic(cls, httpClient, apiBase="/arex", token=None):
        status, text = cls._requestJSONStatic(httpClient, "GET", f"{apiBase}/rest", token=token)
        if status != 200:
            raise ARCHTTPError(status, text)
        apiVersions = json.loads(text)

        # /rest/1.0 compatibility
        if not isinstance(apiVersions["version"], list):
            return [apiVersions["version"]]
        else:
            return apiVersions["version"]

    # TODO: explain the rationale in documentation about the design of the API
    #       version selection mechanism:
    #       - specific API implementations are in classes
    #       - classes cannot be used as values in class variables or method
    #         parameters without the proper ordering of definitions which is
    #         awkward and inflexible
    @classmethod
    def getClient(cls, url=None, host=None, port=None, blocksize=None, timeout=None, token=None, proxypath=None, apiBase="/arex", log=getNullLogger(), version=None, impls=None):
        httpClient = HTTPClient(url, host, port, blocksize, timeout, proxypath, log)

        # get API version to implementation class mapping
        implementations = impls
        if not implementations:
            implementations = cls._getImplementations()

        # get API versions from CE
        apiVersions = cls.getAPIVersionsStatic(httpClient, apiBase, token)
        if not apiVersions:
            raise ARCError("No supported API versions on CE")

        # determine the API version to be used based on available
        # implementations and available versions on ARC CE
        if version:
            if version not in implementations:
                raise ARCError(f"No client support for requested API version {version}")
            if version not in apiVersions:
                raise ARCError(f"API version {version} not among CE supported API versions {apiVersions}")
            apiVersion = version
        else:
            # get the highest version of client implementation supported on the
            # ARC CE
            apiVersion = None
            for version in reversed(apiVersions):
                if version in implementations:
                    apiVersion = version
                    break
            if not apiVersion:
                raise ARCError(f"No client support for CE supported API versions: {apiVersions}")

        log.debug(f"API version {apiVersion} selected")
        return implementations[apiVersion](httpClient, token, proxypath, apiBase, log)

    ### Support methods ###

    def _downloadURL(self, url, path):
        resp = self._request("GET", url)

        if resp.status != 200:
            text = resp.read().decode()
            raise ARCHTTPError(resp.status, text)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        # python 3.6!!!!!!!!!!!!!!
        # TODO: Should this be a standalone attribute of HTTPClient anyway?
        #       Since blocksize is the size of the block output sending buffer
        #       and is probably completely irrelevant to input buffer. Also,
        #       the tuning capabilities would probably require separate
        #       parameters for upload and download ...
        try:
            blocksize = self.httpClient.conn.blocksize
        except:
            blocksize = 8192

        with open(path, "wb") as f:
            data = resp.read(blocksize)
            while data:
                f.write(data)
                data = resp.read(blocksize)

    # returns nothing if match successful, raises exception otherwise
    def _matchQueue(self, ceInfo, queue):
        if not self._findQueue(ceInfo, queue):
            raise MatchmakingError(f"Queue {queue} not found")

    # TODO: is it possible for user to just specify the runtime and any version
    #       is OK or vice versa?
    # returns nothing if match successful, raises exception otherwise
    def _matchRuntime(self, ceInfo, runtime):
        runtimes = self._findRuntimes(ceInfo)
        if runtime not in runtimes:
            raise MatchmakingError(f"Runtime {runtime} not found")

    # returns nothing if match successful, raises exception otherwise
    def _matchWalltime(self, ceInfo, queue, walltime):
        queueInfo = self._findQueue(ceInfo, queue)
        if not queueInfo:
            raise MatchmakingError(f"Queue {queue} not found to match walltime")

        if "MaxWallTime" in queueInfo:
            maxWallTime = int(queueInfo["MaxWallTime"])
            if walltime > maxWallTime:
                raise MatchmakingError(f"Walltime {walltime} higher than max walltime {maxWallTime} for queue {queue}")

    def _findQueue(self, ceInfo, queue):
        compShares = ceInfo.get("Domains", {}) \
                           .get("AdminDomain", {}) \
                           .get("Services", {}) \
                           .get("ComputingService", {}) \
                           .get("ComputingShare", [])
        if not compShares:
            return None

        # /rest/1.0 compatibility
        if not isinstance(compShares, list):
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
        if not isinstance(appenvs, list):
            appenvs = [appenvs]

        runtimes = []
        for env in appenvs:
            if "AppName" in env:
                envname = env["AppName"]
                if "AppVersion" in env:
                    envname += f"-{env['AppVersion']}"
                runtimes.append(envname)
        return runtimes

    def _signCSR(self, csrStr, lifetime=None):
        with open(self.proxypath) as f:
            proxyStr = f.read()
        proxyCert, _, issuerChains = parsePEM(proxyStr)
        chain = proxyCert.public_bytes(serialization.Encoding.PEM).decode() + issuerChains + '\n'
        csr = x509.load_pem_x509_csr(csrStr.encode(), default_backend())
        cert = signRequest(csr, self.proxypath, lifetime=lifetime).decode()
        pem = (cert + chain).encode()
        return pem

    def _addInputTransfers(self, uploadQueue, jobid, inputFiles):
        cancelEvent = threading.Event()
        transfers = []
        for name, source in inputFiles.items():
            try:
                path = isLocalInputFile(name, source)
            except ValueError as exc:
                raise InputFileError(f"Error parsing source {source} of input {name}: {exc}")
            if not path:
                continue
            if not os.path.isfile(path):
                raise InputFileError(f"Source {source} of input {name} is not a file")
            transfers.append(Transfer(jobid, name, path, cancelEvent=cancelEvent))
        # no exception raised, add transfers to queue
        for transfer in transfers:
            uploadQueue.put(transfer)

    def _addDiagnoseTransfers(self, transferQueue, jobid, downloadDir, diagnoseFiles, diagnoseDirs, cancelEvent):
        diagnoseList = diagnoseFiles.get(jobid, self.DIAGNOSE_FILES)
        diagnoseDir = diagnoseDirs.get(jobid, "gmlog")
        transfers = []
        for diagFile in diagnoseList:
            if diagFile not in self.DIAGNOSE_FILES:
                raise ARCError(f"Invalid diagnose file name {diagFile}")
            path = os.path.join(downloadDir, jobid, diagnoseDir, diagFile)
            transfers.append(Transfer(jobid, diagFile, path, type="diagnose", cancelEvent=cancelEvent))
        # no exception raised, add transfers to queue
        for transfer in transfers:
            transferQueue.put(transfer)

    # When name is "", it means the root of the session dir. In this case,
    # slash must not be added to it.
    def _addTransfersFromListing(self, transferQueue, jobid, filters, listing, name, path, cancelEvent):
        files = listing.get("file", [])
        # /rest/1.0 compatibility
        if not isinstance(files, list):
            files = [files]
        for f in files:
            newpath = os.path.join(path, f)
            if name:
                newname = f"{name}/{f}"
            else:
                newname = f
            if not self._filterOutFile(filters, newname):
                transferQueue.put(Transfer(jobid, newname, newpath, type="file", cancelEvent=cancelEvent))

        dirs = listing.get("dirs", [])
        # /rest/1.0 compatibility
        if not isinstance(dirs, list):
            dirs = [dirs]
        for d in dirs:
            newpath = os.path.join(path, d)
            if name:
                newname = f"{name}/{d}"
            else:
                newname = d
            if not self._filterOutListing(filters, newname):
                transferQueue.put(Transfer(jobid, newname, newpath, type="listing", cancelEvent=cancelEvent))

    def _filterOutFile(self, filters, name):
        if not filters:
            return False
        for pattern in filters:
            # direct match
            if pattern == name:
                return False
            # recursive folder match
            elif pattern.endswith("/") and name.startswith(pattern):
                return False
            # entire session directory, not matched by above if
            elif pattern == "/":
                return False
        return True

    def _filterOutListing(self, filters, name):
        if not filters:
            return False
        for pattern in filters:
            # direct match
            if pattern == name or pattern == f"{name}/":
                return False
            # recursive folder match
            elif pattern.endswith("/") and name.startswith(pattern):
                return False
        return True

    def _requestJSON(self, method, endpoint, headers={}, token=None, jsonData=None, data=None, params={}):
        headers["Accept"] = "application/json"
        resp = self._request(method, endpoint, headers, token, jsonData, data, params)
        text = resp.read().decode()
        return resp.status, text

    def _manageJobs(self, jobs, action):
        if not jobs:
            return []

        # JSON data for request
        tomanage = [{"id": job} for job in jobs]

        # /rest/1.0 compatibility
        if len(tomanage) == 1:
            jsonData = {"job": tomanage[0]}
        else:
            jsonData = {"job": tomanage}

        # execute action and get JSON result
        status, text = self._requestJSON("POST", "/jobs", jsonData=jsonData, params={"action": action})
        if status != 201:
            raise ARCHTTPError(status, text)
        jsonData = json.loads(text)

        # /rest/1.0 compatibility
        if not isinstance(jsonData["job"], list):
            return [jsonData["job"]]
        else:
            return jsonData["job"]

    # TODO: think about what to log and how
    def _submitJobs(self, descs, queue, delegationID=None, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None, v1_0=False):
        import arc
        ceInfo = self.getCEInfo()

        if not delegationID:
            delegationID = self.createDelegation()

        # A list of tuples of index and input file dict for every job
        # description to be submitted. The index is the description's
        # position in the given parameter of job descriptions and is
        # required to create properly aligned results.
        tosubmit = []

        # A dict of a key that is index in given descs list and a value that
        # is either a list of exceptions for failed submission or a tuple of
        # jobid and state for successful submission.
        resultDict = {}

        jobdescs = arc.JobDescriptionList()
        bulkdesc = ""
        for i in range(len(descs)):
            # parse job description
            if not arc.JobDescription_Parse(descs[i], jobdescs):
                resultDict[i] = [DescriptionParseError("Failed to parse description")]
                continue
            arcdesc = jobdescs[-1]

            # get queue, runtimes and walltime from description
            jobqueue = arcdesc.Resources.QueueName
            if not jobqueue:
                jobqueue = queue
                if v1_0:
                    # set queue in job description
                    arcdesc.Resources.QueueName = queue
            runtimes = [str(env) for env in arcdesc.Resources.RunTimeEnvironment.getSoftwareList()]
            if not runtimes:
                runtimes = []
            walltime = arcdesc.Resources.TotalWallTime.range.max
            if walltime == -1:
                walltime = None

            # do matchmaking
            if matchDescs:
                errors = self.matchJob(ceInfo, jobqueue, runtimes, walltime)
                if errors:
                    resultDict[i] = errors
                    continue

            if v1_0:
                # add delegation ID to description
                arcdesc.DataStaging.DelegationID = delegationID

            # process job description
            if processDescs:
                self._processJobDescription(arcdesc)

            # get input files from description
            inputFiles = self._getArclibInputFiles(arcdesc)

            # unparse modified description, remove xml version node because it
            # is not accepted by ARC CE, add to bulk description
            unparseResult = arcdesc.UnParse("emies:adl")
            if not unparseResult[0]:
                resultDict[i] = [DescriptionUnparseError("Could not unparse processed description")]
                continue
            descstart = unparseResult[1].find("<ActivityDescription")
            bulkdesc += unparseResult[1][descstart:]

            tosubmit.append((i, inputFiles))

        if not tosubmit:
            return [resultDict[i] for i in range(len(descs))]

        # merge into bulk description
        if len(tosubmit) > 1:
            bulkdesc = f"<ActivityDescriptions>{bulkdesc}</ActivityDescriptions>"

        # submit jobs to ARC
        # TODO: handle exceptions
        results = self.createJobs(bulkdesc, queue, delegationID)

        uploadIXs = []  # a list of job indexes for proper result processing
        uploadIDs = []  # a list of jobids for which to upload files
        uploadInputs = []  # a list of job input file dicts for upload

        for (jobix, inputFiles), result in zip(tosubmit, results):
            if isinstance(result, ARCHTTPError):
                resultDict[jobix] = [result]
            else:
                jobid, state = result
                resultDict[jobix] = (jobid, state)
                uploadIDs.append(jobid)
                uploadInputs.append(inputFiles)
                uploadIXs.append(jobix)

        # upload jobs' local input data
        if uploadData:
            errors = self.uploadJobFiles(uploadIDs, uploadInputs, workers, blocksize, timeout)
            for jobix, uploadErrors in zip(uploadIXs, errors):
                if uploadErrors:
                    jobid, state = resultDict[jobix]
                    resultDict[jobix] = [InputUploadError(jobid, state, uploadErrors)]

        return [resultDict[i] for i in range(len(descs))]

    def _request(self, method, endpoint, headers={}, token=None, jsonData=None, data=None, params={}):
        if not token:
            token = self.token
        endpoint = f"{self.apiPath}{endpoint}"
        return self.httpClient.request(method, endpoint, headers, token, jsonData, data, params)

    ### Static support methods ###

    @classmethod
    def _requestJSONStatic(cls, httpClient, method, endpoint, headers={}, token=None, jsonData=None, data=None, params={}):
        headers["Accept"] = "application/json"
        resp = httpClient.request(method, endpoint, headers, token, jsonData, data, params)
        text = resp.read().decode()
        return resp.status, text

    @classmethod
    def _getImplementations(cls):
        return {"1.0": ARCRest_1_0, "1.1": ARCRest_1_1}

    @classmethod
    def _uploadTransferWorker(cls, restClient, uploadQueue, errorQueue, log=getNullLogger()):
        while True:
            try:
                upload = uploadQueue.get(block=False)
            except queue.Empty:
                break
            uploadQueue.task_done()

            if upload.cancelEvent.is_set():
                log.debug(f"Skipping upload for cancelled job {upload.jobid}")
                continue

            try:
                restClient.uploadFile(upload.jobid, upload.name, upload.path)
            except Exception as exc:
                upload.cancelEvent.set()
                errorQueue.put({"jobid": upload.jobid, "error": exc})
                log.debug(f"Error uploading {upload.path} for job {upload.jobid}: {exc}")

    # TODO: add bail out parameter for cancelEvent?
    @classmethod
    def _downloadTransferWorker(cls, restClient, transferQueue, errorQueue, downloadDir, outputFilters={}, log=getNullLogger()):
        while True:
            try:
                transfer = transferQueue.get()
            except TransferQueueEmpty:
                break

            jobid, name, path = transfer.jobid, transfer.name, transfer.path
            if transfer.cancelEvent.is_set():
                log.debug(f"Skipping download for cancelled job {jobid}")
                continue

            try:
                if transfer.type in ("file", "diagnose"):
                    try:
                        if transfer.type == "file":
                            restClient.downloadFile(jobid, name, path)
                        elif transfer.type == "diagnose":
                            restClient.downloadDiagnoseFile(jobid, name, path)
                    except Exception as exc:
                        error = exc
                        if isinstance(exc, ARCHTTPError):
                            if exc.status == 404:
                                if transfer.type == "file":
                                    error = MissingOutputFile(name)
                                elif transfer.type == "diagnose":
                                    error = MissingDiagnoseFile(name)
                        errorQueue.put({"jobid": jobid, "error": error})
                        log.error(f"Download {transfer.type} {name} to {path} for job {jobid} failed: {error}")

                elif transfer.type == "listing":
                    try:
                        listing = restClient.downloadListing(jobid, name)
                    except Exception as exc:
                        errorQueue.put({"jobid": jobid, "error": exc})
                        log.error(f"Download listing {name} for job {jobid} failed: {exc}")
                    else:
                        filters = outputFilters.get(jobid, [])
                        # create new transfer jobs
                        restClient._addTransfersFromListing(
                            transferQueue, jobid, filters, listing, name, path, transfer.cancelEvent,
                        )

            # every possible exception needs to be handled, otherwise the
            # threads will lock up
            except:
                import traceback
                excstr = traceback.format_exc()
                errorQueue.put({"jobid": jobid, "error": Exception(excstr)})
                log.debug(f"Download name {name} and path {path} for job {jobid} failed: {excstr}")

    @classmethod
    def _getArclibInputFiles(cls, desc):
        inputFiles = {}
        for infile in desc.DataStaging.InputFiles:
            source = None
            if len(infile.Sources) > 0:
                source = infile.Sources[0].fullstr()
            inputFiles[infile.Name] = source
        return inputFiles

    @classmethod
    def _processJobDescription(cls, jobdesc):
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

    @classmethod
    def _parseJobInfo(cls, infoDocument):
        jobInfo = {}
        infoDict = infoDocument.get("ComputingActivity", {})

        COPY_KEYS = ["Name", "Type", "LocalIDFromManager", "Owner", "LocalOwner", "StdIn", "StdOut", "StdErr", "LogDir", "Queue"]
        for key in COPY_KEYS:
            if key in infoDict:
                jobInfo[key] = infoDict[key]

        INT_KEYS = ["UsedTotalWallTime", "UsedTotalCPUTime", "RequestedTotalWallTime", "RequestedTotalCPUTime", "RequestedSlots", "ExitCode", "WaitingPosition", "UsedMainMemory"]
        for key in INT_KEYS:
            if key in infoDict:
                jobInfo[key] = int(infoDict[key])

        TSTAMP_KEYS = ["SubmissionTime", "EndTime", "WorkingAreaEraseTime", "ProxyExpirationTime"]
        for key in TSTAMP_KEYS:
            if key in infoDict:
                jobInfo[key] = datetime.datetime.strptime(infoDict[key], "%Y-%m-%dT%H:%M:%SZ")

        VARIABLE_KEYS = ["Error", "ExecutionNode"]
        for key in VARIABLE_KEYS:
            if key in infoDict:
                jobInfo[key] = infoDict[key]
                # /rest/1.0 compatibility
                if not isinstance(jobInfo[key], list):
                    jobInfo[key] = [jobInfo[key]]

        states = infoDict.get("State", [])
        # /rest/1.0 compatibility
        if not isinstance(states, list):
            states = [states]
        # get state from a list of states in different systems
        for state in states:
            if state.startswith("arcrest:"):
                jobInfo["state"] = state[len("arcrest:"):]

        restartStates = infoDict.get("RestartState", [])
        # /rest/1.0 compatibility
        if not isinstance(restartStates, list):
            restartStates = [restartStates]
        # get restart state from a list of restart states in different systems
        for state in restartStates:
            if state.startswith("arcrest:"):
                jobInfo["restartState"] = state[len("arcrest:"):]

        return jobInfo


class ARCRest_1_0(ARCRest):

    def __init__(self, httpClient, token=None, proxypath=None, apiBase="/arex", log=getNullLogger()):
        if token:
            raise ARCError("Token credentials not supported for API version 1.0")
        super().__init__(httpClient, None, proxypath, apiBase, log)
        self.version = "1.0"
        self.apiPath = f"{self.apiBase}/rest/{self.version}"

    def createJobs(self, description, queue=None, delegationID=None, isADL=True):
        contentType = "application/xml" if isADL else "application/rsl"
        status, text = self._requestJSON(
            "POST",
            "/jobs",
            data=description,
            headers={"Content-Type": contentType},
            params={"action": "new"},
        )
        if status != 201:
            raise ARCHTTPError(status, text)
        jsonData = json.loads(text)

        # /rest/1.0 compatibility
        if not isinstance(jsonData["job"], list):
            responses = [jsonData["job"]]
        else:
            responses = jsonData["job"]

        results = []
        for response in responses:
            code, reason = int(response["status-code"]), response["reason"]
            if code != 201:
                results.append(ARCHTTPError(code, reason))
            else:
                results.append((response["id"], response["state"]))
        return results

    def submitJobs(self, descs, queue, delegationID=None, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None):
        return self._submitJobs(descs, queue, delegationID, processDescs, matchDescs, uploadData, workers, blocksize, timeout, v1_0=True)

    def getDelegationsList(self, type=None):
        return super().getDelegationsList(type=None)


class ARCRest_1_1(ARCRest):

    def __init__(self, httpClient, token=None, proxypath=None, apiBase="/arex", log=getNullLogger()):
        super().__init__(httpClient, token, proxypath, apiBase, log)
        self.version = "1.1"
        self.apiPath = f"{self.apiBase}/rest/{self.version}"

    def createJobs(self, description, queue=None, delegationID=None, isADL=True):
        params = {"action": "new"}
        if queue:
            params["queue"] = queue
        if delegationID:
            params["delegation_id"] = delegationID
        headers = {"Content-Type": "application/xml" if isADL else "application/rsl"}
        status, text = self._requestJSON(
            "POST",
            "/jobs",
            data=description,
            headers=headers,
            params=params,
        )
        if status != 201:
            raise ARCHTTPError(status, text)
        responses = json.loads(text)["job"]

        results = []
        for response in responses:
            code, reason = int(response["status-code"]), response["reason"]
            if code != 201:
                results.append(ARCHTTPError(code, reason))
            else:
                results.append((response["id"], response["state"]))
        return results

    def submitJobs(self, descs, queue, delegationID=None, processDescs=True, matchDescs=True, uploadData=True, workers=10, blocksize=None, timeout=None):
        return self._submitJobs(descs, queue, delegationID, processDescs, matchDescs, uploadData, workers, blocksize, timeout)


class Transfer:

    def __init__(self, jobid, name, path, type="file", cancelEvent=None):
        self.jobid = jobid
        self.name = name
        self.path = path
        self.type = type
        self.cancelEvent = cancelEvent
        if not self.cancelEvent:
            self.cancelEvent = threading.Event()


class ARCJob:

    def __init__(self, id=None, descstr=None):
        self.id = id
        self.descstr = descstr
        self.name = None
        self.delegid = None
        self.state = None
        self.errors = []
        self.downloadFiles = []
        self.inputFiles = {}

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
        self.RestartState = []
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

        if "RestartState" in infoDict:
            self.RestartState = infoDict["RestartState"]

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


class TransferQueueEmpty(Exception):
    pass


def isLocalInputFile(name, source):
    """
    Return path if local or empty string if remote URL.

    Raises:
        - ValueError: source cannot be parsed
    """
    if not source:
        return name
    url = urlparse(source)
    if url.scheme not in ("file", None, "") or url.hostname:
        return ""
    return url.path
