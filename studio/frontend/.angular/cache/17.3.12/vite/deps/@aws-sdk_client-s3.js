import {
  AwsCrc32,
  BinaryDecisionDiagram,
  Client,
  Command,
  EndpointCache,
  FromStringShapeDeserializer,
  HttpBindingProtocol,
  HttpInterceptingShapeDeserializer,
  HttpInterceptingShapeSerializer,
  HttpRequest,
  HttpResponse,
  NoOpLogger,
  NormalizedSchema,
  NumericValue,
  ServiceException,
  TypeRegistry,
  WaiterState,
  buildQueryString,
  checkExceptions,
  createAggregatedClient,
  createBufferedReadable,
  createChecksumStream,
  createWaiter,
  customEndpointFunctions,
  dateToUtcString,
  decideEndpoint,
  decorateServiceException,
  determineTimestampFormat,
  escapeUri,
  eventStreamSerdeProvider2 as eventStreamSerdeProvider,
  fromBase64,
  fromHex,
  fromUtf8 as fromUtf82,
  generateIdempotencyToken,
  getAwsChunkedEncodingStream,
  getContentLengthPlugin,
  getDefaultExtensionConfiguration,
  getEndpointPlugin,
  getHttpHandlerExtensionConfiguration,
  getSchemaSerdePlugin,
  getSmithyContext,
  getValueFromTextNode,
  invalidProvider,
  isArrayBuffer,
  isIpAddress,
  isValidHostLabel,
  loadConfigsForDefaultMode,
  normalizeProvider,
  parseRfc7231DateTime,
  parseUrl,
  resolveDefaultRuntimeConfig,
  resolveEndpointConfig,
  resolveEventStreamSerdeConfig,
  resolveHttpHandlerRuntimeConfig,
  resolveParams,
  sdkStreamMixin,
  toBase64,
  toHex,
  toUint8Array,
  toUtf8,
  v4
} from "./chunk-A3AJAY4X.js";
import {
  Sha256
} from "./chunk-Z2DDE6C6.js";
import {
  convertToBuffer,
  fromUtf8,
  isEmptyData,
  numToUint8,
  uint32ArrayFrom
} from "./chunk-F6RSW7MC.js";
import {
  __awaiter,
  __generator,
  __values
} from "./chunk-ZA2DQWUL.js";
import {
  __async,
  __asyncGenerator,
  __await,
  __objRest,
  __spreadProps,
  __spreadValues,
  __superGet
} from "./chunk-JHTNPNKS.js";

// node_modules/@aws-sdk/middleware-expect-continue/dist-es/index.js
function addExpectContinueMiddleware(options) {
  return (next) => (args) => __async(this, null, function* () {
    const { request } = args;
    if (options.expectContinueHeader !== false && HttpRequest.isInstance(request) && request.body && options.runtime === "node" && options.requestHandler?.constructor?.name !== "FetchHttpHandler") {
      let sendHeader = true;
      if (typeof options.expectContinueHeader === "number") {
        try {
          const bodyLength = Number(request.headers?.["content-length"]) ?? options.bodyLengthChecker?.(request.body) ?? Infinity;
          sendHeader = bodyLength >= options.expectContinueHeader;
        } catch (e2) {
        }
      } else {
        sendHeader = !!options.expectContinueHeader;
      }
      if (sendHeader) {
        request.headers.Expect = "100-continue";
      }
    }
    return next(__spreadProps(__spreadValues({}, args), {
      request
    }));
  });
}
var addExpectContinueMiddlewareOptions = {
  step: "build",
  tags: ["SET_EXPECT_HEADER", "EXPECT_HEADER"],
  name: "addExpectContinueMiddleware",
  override: true
};
var getAddExpectContinuePlugin = (options) => ({
  applyToStack: (clientStack) => {
    clientStack.add(addExpectContinueMiddleware(options), addExpectContinueMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/constants.js
var RequestChecksumCalculation = {
  WHEN_SUPPORTED: "WHEN_SUPPORTED",
  WHEN_REQUIRED: "WHEN_REQUIRED"
};
var DEFAULT_REQUEST_CHECKSUM_CALCULATION = RequestChecksumCalculation.WHEN_SUPPORTED;
var ResponseChecksumValidation = {
  WHEN_SUPPORTED: "WHEN_SUPPORTED",
  WHEN_REQUIRED: "WHEN_REQUIRED"
};
var DEFAULT_RESPONSE_CHECKSUM_VALIDATION = RequestChecksumCalculation.WHEN_SUPPORTED;
var ChecksumAlgorithm;
(function(ChecksumAlgorithm3) {
  ChecksumAlgorithm3["MD5"] = "MD5";
  ChecksumAlgorithm3["CRC32"] = "CRC32";
  ChecksumAlgorithm3["CRC32C"] = "CRC32C";
  ChecksumAlgorithm3["CRC64NVME"] = "CRC64NVME";
  ChecksumAlgorithm3["SHA1"] = "SHA1";
  ChecksumAlgorithm3["SHA256"] = "SHA256";
})(ChecksumAlgorithm || (ChecksumAlgorithm = {}));
var ChecksumLocation;
(function(ChecksumLocation2) {
  ChecksumLocation2["HEADER"] = "header";
  ChecksumLocation2["TRAILER"] = "trailer";
})(ChecksumLocation || (ChecksumLocation = {}));
var DEFAULT_CHECKSUM_ALGORITHM = ChecksumAlgorithm.CRC32;

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/stringUnionSelector.js
var SelectorType;
(function(SelectorType4) {
  SelectorType4["ENV"] = "env";
  SelectorType4["CONFIG"] = "shared config entry";
})(SelectorType || (SelectorType = {}));

// node_modules/@aws-sdk/core/dist-es/submodules/client/setCredentialFeature.js
function setCredentialFeature(credentials, feature, value) {
  if (!credentials.$source) {
    credentials.$source = {};
  }
  credentials.$source[feature] = value;
  return credentials;
}

// node_modules/@smithy/core/dist-es/submodules/retry/middleware-retry/isStreamingPayload/isStreamingPayload.browser.js
var isStreamingPayload = (request) => request?.body instanceof ReadableStream;

// node_modules/@smithy/core/dist-es/submodules/retry/service-error-classification/constants.js
var THROTTLING_ERROR_CODES = [
  "BandwidthLimitExceeded",
  "EC2ThrottledException",
  "LimitExceededException",
  "PriorRequestNotComplete",
  "ProvisionedThroughputExceededException",
  "RequestLimitExceeded",
  "RequestThrottled",
  "RequestThrottledException",
  "SlowDown",
  "ThrottledException",
  "Throttling",
  "ThrottlingException",
  "TooManyRequestsException",
  "TransactionInProgressException"
];
var TRANSIENT_ERROR_CODES = ["TimeoutError", "RequestTimeout", "RequestTimeoutException"];
var TRANSIENT_ERROR_STATUS_CODES = [500, 502, 503, 504];
var NODEJS_TIMEOUT_ERROR_CODES = ["ECONNRESET", "ECONNREFUSED", "EPIPE", "ETIMEDOUT"];
var NODEJS_NETWORK_ERROR_CODES = ["EHOSTUNREACH", "ENETUNREACH", "ENOTFOUND"];

// node_modules/@smithy/core/dist-es/submodules/retry/service-error-classification/service-error-classification.js
var isRetryableByTrait = (error) => error?.$retryable !== void 0;
var isClockSkewCorrectedError = (error) => error.$metadata?.clockSkewCorrected;
var isBrowserNetworkError = (error) => {
  const errorMessages = /* @__PURE__ */ new Set([
    "Failed to fetch",
    "NetworkError when attempting to fetch resource",
    "The Internet connection appears to be offline",
    "Load failed",
    "Network request failed"
  ]);
  const isValid = error && error instanceof TypeError;
  if (!isValid) {
    return false;
  }
  return errorMessages.has(error.message);
};
var isThrottlingError = (error) => error.$metadata?.httpStatusCode === 429 || THROTTLING_ERROR_CODES.includes(error.name) || error.$retryable?.throttling == true;
var isTransientError = (error, depth = 0) => isRetryableByTrait(error) || isClockSkewCorrectedError(error) || error.name === "InvalidSignatureException" && error.message?.includes("Signature expired") || TRANSIENT_ERROR_CODES.includes(error.name) || NODEJS_TIMEOUT_ERROR_CODES.includes(error?.code || "") || NODEJS_NETWORK_ERROR_CODES.includes(error?.code || "") || TRANSIENT_ERROR_STATUS_CODES.includes(error.$metadata?.httpStatusCode || 0) || isBrowserNetworkError(error) || isNodeJsHttp2TransientError(error) || error.cause !== void 0 && depth <= 10 && isTransientError(error.cause, depth + 1);
var isServerError = (error) => {
  if (error.$metadata?.httpStatusCode !== void 0) {
    const statusCode = error.$metadata.httpStatusCode;
    if (500 <= statusCode && statusCode <= 599 && !isTransientError(error)) {
      return true;
    }
    return false;
  }
  return false;
};
function isNodeJsHttp2TransientError(error) {
  return error.code === "ERR_HTTP2_STREAM_ERROR" && error.message.includes("NGHTTP2_REFUSED_STREAM");
}

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/constants.js
var MAXIMUM_RETRY_DELAY = 20 * 1e3;
var INITIAL_RETRY_TOKENS = 500;
var NO_RETRY_INCREMENT = 1;
var INVOCATION_ID_HEADER = "amz-sdk-invocation-id";
var REQUEST_HEADER = "amz-sdk-request";

// node_modules/@smithy/core/dist-es/submodules/retry/middleware-retry/parseRetryAfterHeader.js
function parseRetryAfterHeader(response, logger2) {
  if (!HttpResponse.isInstance(response)) {
    return;
  }
  for (const header of Object.keys(response.headers)) {
    const h2 = header.toLowerCase();
    if (h2 === "retry-after") {
      const retryAfter = response.headers[header];
      let retryAfterSeconds = NaN;
      if (retryAfter.endsWith("GMT")) {
        try {
          const date = parseRfc7231DateTime(retryAfter);
          retryAfterSeconds = (date.getTime() - Date.now()) / 1e3;
        } catch (e2) {
          logger2?.trace?.("Failed to parse retry-after header");
          logger2?.trace?.(e2);
        }
      } else if (retryAfter.match(/ GMT, ((\d+)|(\d+\.\d+))$/)) {
        retryAfterSeconds = Number(retryAfter.match(/ GMT, ([\d.]+)$/)?.[1]);
      } else if (retryAfter.match(/^((\d+)|(\d+\.\d+))$/)) {
        retryAfterSeconds = Number(retryAfter);
      } else if (Date.parse(retryAfter) >= Date.now()) {
        retryAfterSeconds = (Date.parse(retryAfter) - Date.now()) / 1e3;
      }
      if (isNaN(retryAfterSeconds)) {
        return;
      }
      return new Date(Date.now() + retryAfterSeconds * 1e3);
    } else if (h2 === "x-amz-retry-after") {
      const v2 = response.headers[header];
      const backoffMilliseconds = Number(v2);
      if (isNaN(backoffMilliseconds)) {
        logger2?.trace?.(`Failed to parse x-amz-retry-after=${v2}`);
        return;
      }
      return new Date(Date.now() + backoffMilliseconds);
    }
  }
}

// node_modules/@smithy/core/dist-es/submodules/retry/middleware-retry/util.js
var asSdkError = (error) => {
  if (error instanceof Error)
    return error;
  if (error instanceof Object)
    return Object.assign(new Error(), error);
  if (typeof error === "string")
    return new Error(error);
  return new Error(`AWS SDK error wrapper for ${error}`);
};

// node_modules/@smithy/core/dist-es/submodules/retry/middleware-retry/retryMiddleware.js
function bindRetryMiddleware(isStreamingPayload2) {
  return (options) => (next, context) => (args) => __async(this, null, function* () {
    let retryStrategy = yield options.retryStrategy();
    const maxAttempts = yield options.maxAttempts();
    if (isRetryStrategyV2(retryStrategy)) {
      retryStrategy = retryStrategy;
      let retryToken = yield retryStrategy.acquireInitialRetryToken((context["partition_id"] ?? "") + (context.__retryLongPoll ? ":longpoll" : ""));
      let lastError = new Error();
      let attempts = 0;
      let totalRetryDelay = 0;
      const { request } = args;
      const isRequest = HttpRequest.isInstance(request);
      if (isRequest) {
        request.headers[INVOCATION_ID_HEADER] = v4();
      }
      while (true) {
        try {
          if (isRequest) {
            request.headers[REQUEST_HEADER] = `attempt=${attempts + 1}; max=${maxAttempts}`;
          }
          const { response, output } = yield next(args);
          retryStrategy.recordSuccess(retryToken);
          output.$metadata.attempts = attempts + 1;
          output.$metadata.totalRetryDelay = totalRetryDelay;
          return { response, output };
        } catch (e2) {
          const retryErrorInfo = getRetryErrorInfo(e2, options.logger);
          lastError = asSdkError(e2);
          if (isRequest && isStreamingPayload2(request)) {
            (context.logger instanceof NoOpLogger ? console : context.logger)?.warn("An error was encountered in a non-retryable streaming request.");
            throw lastError;
          }
          try {
            retryToken = yield retryStrategy.refreshRetryTokenForRetry(retryToken, retryErrorInfo);
          } catch (refreshError) {
            if (typeof refreshError.$backoff === "number") {
              yield cooldown(refreshError.$backoff);
            }
            if (!lastError.$metadata) {
              lastError.$metadata = {};
            }
            lastError.$metadata.attempts = attempts + 1;
            lastError.$metadata.totalRetryDelay = totalRetryDelay;
            throw lastError;
          }
          attempts = retryToken.getRetryCount();
          const delay = retryToken.getRetryDelay();
          totalRetryDelay += delay;
          yield cooldown(delay);
        }
      }
    } else {
      retryStrategy = retryStrategy;
      if (retryStrategy?.mode) {
        context.userAgent = [...context.userAgent || [], ["cfg/retry-mode", retryStrategy.mode]];
      }
      return retryStrategy.retry(next, args);
    }
  });
}
var cooldown = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
var isRetryStrategyV2 = (retryStrategy) => typeof retryStrategy.acquireInitialRetryToken !== "undefined" && typeof retryStrategy.refreshRetryTokenForRetry !== "undefined" && typeof retryStrategy.recordSuccess !== "undefined";
var getRetryErrorInfo = (error, logger2) => {
  const errorInfo = {
    error,
    errorType: getRetryErrorType(error)
  };
  const retryAfterHint = parseRetryAfterHeader(error.$response, logger2);
  if (retryAfterHint) {
    errorInfo.retryAfterHint = retryAfterHint;
  }
  return errorInfo;
};
var getRetryErrorType = (error) => {
  if (isThrottlingError(error))
    return "THROTTLING";
  if (isTransientError(error))
    return "TRANSIENT";
  if (isServerError(error))
    return "SERVER_ERROR";
  return "CLIENT_ERROR";
};
var retryMiddlewareOptions = {
  name: "retryMiddleware",
  tags: ["RETRY"],
  step: "finalizeRequest",
  priority: "high",
  override: true
};
function bindGetRetryPlugin(isStreamingPayload2) {
  const retryMiddleware2 = bindRetryMiddleware(isStreamingPayload2);
  return (options) => ({
    applyToStack: (clientStack) => {
      clientStack.add(retryMiddleware2(options), retryMiddlewareOptions);
    }
  });
}

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/DefaultRateLimiter.js
var DefaultRateLimiter = class _DefaultRateLimiter {
  static setTimeoutFn = setTimeout;
  beta;
  minCapacity;
  minFillRate;
  scaleConstant;
  smooth;
  enabled = false;
  availableTokens = 0;
  lastMaxRate = 0;
  measuredTxRate = 0;
  requestCount = 0;
  fillRate;
  lastThrottleTime;
  lastTimestamp = 0;
  lastTxRateBucket;
  maxCapacity;
  timeWindow = 0;
  constructor(options) {
    this.beta = options?.beta ?? 0.7;
    this.minCapacity = options?.minCapacity ?? 1;
    this.minFillRate = options?.minFillRate ?? 0.5;
    this.scaleConstant = options?.scaleConstant ?? 0.4;
    this.smooth = options?.smooth ?? 0.8;
    this.lastThrottleTime = this.getCurrentTimeInSeconds();
    this.lastTxRateBucket = Math.floor(this.getCurrentTimeInSeconds());
    this.fillRate = this.minFillRate;
    this.maxCapacity = this.minCapacity;
  }
  getSendToken() {
    return __async(this, null, function* () {
      return this.acquireTokenBucket(1);
    });
  }
  updateClientSendingRate(response) {
    let calculatedRate;
    this.updateMeasuredRate();
    const retryErrorInfo = response;
    const isThrottling = retryErrorInfo?.errorType === "THROTTLING" || isThrottlingError(retryErrorInfo?.error ?? response);
    if (isThrottling) {
      const rateToUse = !this.enabled ? this.measuredTxRate : Math.min(this.measuredTxRate, this.fillRate);
      this.lastMaxRate = rateToUse;
      this.calculateTimeWindow();
      this.lastThrottleTime = this.getCurrentTimeInSeconds();
      calculatedRate = this.cubicThrottle(rateToUse);
      this.enableTokenBucket();
    } else {
      this.calculateTimeWindow();
      calculatedRate = this.cubicSuccess(this.getCurrentTimeInSeconds());
    }
    const newRate = Math.min(calculatedRate, 2 * this.measuredTxRate);
    this.updateTokenBucketRate(newRate);
  }
  getCurrentTimeInSeconds() {
    return Date.now() / 1e3;
  }
  acquireTokenBucket(amount) {
    return __async(this, null, function* () {
      if (!this.enabled) {
        return;
      }
      this.refillTokenBucket();
      while (amount > this.availableTokens) {
        const delay = (amount - this.availableTokens) / this.fillRate * 1e3;
        yield new Promise((resolve) => _DefaultRateLimiter.setTimeoutFn(resolve, delay));
        this.refillTokenBucket();
      }
      this.availableTokens = this.availableTokens - amount;
    });
  }
  refillTokenBucket() {
    const timestamp = this.getCurrentTimeInSeconds();
    if (!this.lastTimestamp) {
      this.lastTimestamp = timestamp;
      return;
    }
    const fillAmount = (timestamp - this.lastTimestamp) * this.fillRate;
    this.availableTokens = Math.min(this.maxCapacity, this.availableTokens + fillAmount);
    this.lastTimestamp = timestamp;
  }
  calculateTimeWindow() {
    this.timeWindow = this.getPrecise(Math.pow(this.lastMaxRate * (1 - this.beta) / this.scaleConstant, 1 / 3));
  }
  cubicThrottle(rateToUse) {
    return this.getPrecise(rateToUse * this.beta);
  }
  cubicSuccess(timestamp) {
    return this.getPrecise(this.scaleConstant * Math.pow(timestamp - this.lastThrottleTime - this.timeWindow, 3) + this.lastMaxRate);
  }
  enableTokenBucket() {
    this.enabled = true;
  }
  updateTokenBucketRate(newRate) {
    this.refillTokenBucket();
    this.fillRate = Math.max(newRate, this.minFillRate);
    this.maxCapacity = Math.max(newRate, this.minCapacity);
    this.availableTokens = Math.min(this.availableTokens, this.maxCapacity);
  }
  updateMeasuredRate() {
    const t8 = this.getCurrentTimeInSeconds();
    const timeBucket = Math.floor(t8 * 2) / 2;
    this.requestCount++;
    if (timeBucket > this.lastTxRateBucket) {
      const currentRate = this.requestCount / (timeBucket - this.lastTxRateBucket);
      this.measuredTxRate = this.getPrecise(currentRate * this.smooth + this.measuredTxRate * (1 - this.smooth));
      this.requestCount = 0;
      this.lastTxRateBucket = timeBucket;
    }
  }
  getPrecise(num) {
    return parseFloat(num.toFixed(8));
  }
};

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/retries-2026-config.js
var Retry = class _Retry {
  static v2026 = typeof process !== "undefined" && process.env?.SMITHY_NEW_RETRIES_2026 === "true";
  static delay() {
    return _Retry.v2026 ? 50 : 100;
  }
  static throttlingDelay() {
    return _Retry.v2026 ? 1e3 : 500;
  }
  static cost() {
    return _Retry.v2026 ? 14 : 5;
  }
  static throttlingCost() {
    return _Retry.v2026 ? 5 : 10;
  }
  static modifiedCostType() {
    return _Retry.v2026 ? "THROTTLING" : "TRANSIENT";
  }
};

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/DefaultRetryBackoffStrategy.js
var DefaultRetryBackoffStrategy = class {
  x = Retry.delay();
  computeNextBackoffDelay(i2) {
    const b2 = Math.random();
    const r2 = 2;
    const t_i = b2 * Math.min(this.x * r2 ** i2, MAXIMUM_RETRY_DELAY);
    return Math.floor(t_i);
  }
  setDelayBase(delay) {
    this.x = delay;
  }
};

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/DefaultRetryToken.js
var DefaultRetryToken = class {
  delay;
  count;
  cost;
  longPoll;
  constructor(delay, count, cost, longPoll) {
    this.delay = delay;
    this.count = count;
    this.cost = cost;
    this.longPoll = longPoll;
  }
  getRetryCount() {
    return this.count;
  }
  getRetryDelay() {
    return Math.min(MAXIMUM_RETRY_DELAY, this.delay);
  }
  getRetryCost() {
    return this.cost;
  }
  isLongPoll() {
    return this.longPoll;
  }
};

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/config.js
var RETRY_MODES;
(function(RETRY_MODES2) {
  RETRY_MODES2["STANDARD"] = "standard";
  RETRY_MODES2["ADAPTIVE"] = "adaptive";
})(RETRY_MODES || (RETRY_MODES = {}));
var DEFAULT_MAX_ATTEMPTS = 3;
var DEFAULT_RETRY_MODE = RETRY_MODES.STANDARD;

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/StandardRetryStrategy.js
var refusal = {
  incompatible: 1,
  attempts: 2,
  capacity: 3
};
var StandardRetryStrategy = class {
  mode = RETRY_MODES.STANDARD;
  capacity = INITIAL_RETRY_TOKENS;
  retryBackoffStrategy;
  maxAttemptsProvider;
  baseDelay;
  constructor(arg1) {
    if (typeof arg1 === "number") {
      this.maxAttemptsProvider = () => __async(this, null, function* () {
        return arg1;
      });
    } else if (typeof arg1 === "function") {
      this.maxAttemptsProvider = arg1;
    } else if (arg1 && typeof arg1 === "object") {
      this.maxAttemptsProvider = () => __async(this, null, function* () {
        return arg1.maxAttempts;
      });
      this.baseDelay = arg1.baseDelay;
      this.retryBackoffStrategy = arg1.backoff;
    }
    this.maxAttemptsProvider ??= () => __async(this, null, function* () {
      return DEFAULT_MAX_ATTEMPTS;
    });
    this.baseDelay ??= Retry.delay();
    this.retryBackoffStrategy ??= new DefaultRetryBackoffStrategy();
  }
  acquireInitialRetryToken(retryTokenScope) {
    return __async(this, null, function* () {
      return new DefaultRetryToken(Retry.delay(), 0, void 0, Retry.v2026 && retryTokenScope.includes(":longpoll"));
    });
  }
  refreshRetryTokenForRetry(token, errorInfo) {
    return __async(this, null, function* () {
      const maxAttempts = yield this.getMaxAttempts();
      const retryCode = this.retryCode(token, errorInfo, maxAttempts);
      const shouldRetry = retryCode === 0;
      const isLongPoll = token.isLongPoll?.();
      if (shouldRetry || isLongPoll) {
        const errorType = errorInfo.errorType;
        this.retryBackoffStrategy.setDelayBase(errorType === "THROTTLING" ? Retry.throttlingDelay() : this.baseDelay);
        const delayFromErrorType = this.retryBackoffStrategy.computeNextBackoffDelay(token.getRetryCount());
        let retryDelay = delayFromErrorType;
        if (errorInfo.retryAfterHint instanceof Date) {
          retryDelay = Math.max(delayFromErrorType, Math.min(errorInfo.retryAfterHint.getTime() - Date.now(), delayFromErrorType + 5e3));
        }
        if (!shouldRetry) {
          throw Object.assign(new Error("No retry token available"), {
            $backoff: Retry.v2026 && retryCode === refusal.capacity && isLongPoll ? retryDelay : 0
          });
        } else {
          const capacityCost = this.getCapacityCost(errorType);
          this.capacity -= capacityCost;
          return new DefaultRetryToken(retryDelay, token.getRetryCount() + 1, capacityCost, token.isLongPoll?.() ?? false);
        }
      }
      throw new Error("No retry token available");
    });
  }
  recordSuccess(token) {
    this.capacity = Math.min(INITIAL_RETRY_TOKENS, this.capacity + (token.getRetryCost() ?? NO_RETRY_INCREMENT));
  }
  getCapacity() {
    return this.capacity;
  }
  maxAttempts() {
    return __async(this, null, function* () {
      return this.maxAttemptsProvider();
    });
  }
  getMaxAttempts() {
    return __async(this, null, function* () {
      try {
        return yield this.maxAttemptsProvider();
      } catch (error) {
        console.warn(`Max attempts provider could not resolve. Using default of ${DEFAULT_MAX_ATTEMPTS}`);
        return DEFAULT_MAX_ATTEMPTS;
      }
    });
  }
  retryCode(tokenToRenew, errorInfo, maxAttempts) {
    const attempts = tokenToRenew.getRetryCount() + 1;
    const retryableStatus = this.isRetryableError(errorInfo.errorType) ? 0 : refusal.incompatible;
    const attemptStatus = attempts < maxAttempts ? 0 : refusal.attempts;
    const capacityStatus = this.capacity >= this.getCapacityCost(errorInfo.errorType) ? 0 : refusal.capacity;
    return retryableStatus || attemptStatus || capacityStatus;
  }
  getCapacityCost(errorType) {
    return errorType === Retry.modifiedCostType() ? Retry.throttlingCost() : Retry.cost();
  }
  isRetryableError(errorType) {
    return errorType === "THROTTLING" || errorType === "TRANSIENT";
  }
};

// node_modules/@smithy/core/dist-es/submodules/retry/util-retry/AdaptiveRetryStrategy.js
var AdaptiveRetryStrategy = class {
  mode = RETRY_MODES.ADAPTIVE;
  rateLimiter;
  standardRetryStrategy;
  constructor(maxAttemptsProvider, options) {
    const { rateLimiter } = options ?? {};
    this.rateLimiter = rateLimiter ?? new DefaultRateLimiter();
    this.standardRetryStrategy = options ? new StandardRetryStrategy(__spreadValues({
      maxAttempts: typeof maxAttemptsProvider === "number" ? maxAttemptsProvider : 3
    }, options)) : new StandardRetryStrategy(maxAttemptsProvider);
  }
  acquireInitialRetryToken(retryTokenScope) {
    return __async(this, null, function* () {
      const token = yield this.standardRetryStrategy.acquireInitialRetryToken(retryTokenScope);
      yield this.rateLimiter.getSendToken();
      return token;
    });
  }
  refreshRetryTokenForRetry(tokenToRenew, errorInfo) {
    return __async(this, null, function* () {
      this.rateLimiter.updateClientSendingRate(errorInfo);
      const token = yield this.standardRetryStrategy.refreshRetryTokenForRetry(tokenToRenew, errorInfo);
      yield this.rateLimiter.getSendToken();
      return token;
    });
  }
  recordSuccess(token) {
    this.rateLimiter.updateClientSendingRate({});
    this.standardRetryStrategy.recordSuccess(token);
  }
  maxAttemptsProvider() {
    return __async(this, null, function* () {
      return this.standardRetryStrategy.maxAttempts();
    });
  }
};

// node_modules/@smithy/core/dist-es/submodules/retry/middleware-retry/configurations.js
var resolveRetryConfig = (input) => {
  const { retryStrategy, retryMode } = input;
  const maxAttempts = normalizeProvider(input.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
  let controller = retryStrategy ? Promise.resolve(retryStrategy) : void 0;
  const getDefault = () => __async(void 0, null, function* () {
    return (yield normalizeProvider(retryMode)()) === RETRY_MODES.ADAPTIVE ? new AdaptiveRetryStrategy(maxAttempts) : new StandardRetryStrategy(maxAttempts);
  });
  return Object.assign(input, {
    maxAttempts,
    retryStrategy: () => controller ??= getDefault()
  });
};

// node_modules/@smithy/core/dist-es/submodules/retry/index.browser.js
var no = Symbol.for("node-only");
var retryMiddleware = bindRetryMiddleware(isStreamingPayload);
var getRetryPlugin = bindGetRetryPlugin(isStreamingPayload);

// node_modules/@aws-sdk/core/dist-es/submodules/client/setFeature.js
Retry.v2026 ||= typeof process === "object" && process.env?.AWS_NEW_RETRIES_2026 === "true";
function setFeature(context, feature, value) {
  if (!context.__aws_sdk_context) {
    context.__aws_sdk_context = {
      features: {}
    };
  } else if (!context.__aws_sdk_context.features) {
    context.__aws_sdk_context.features = {};
  }
  context.__aws_sdk_context.features[feature] = value;
}

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/getChecksumAlgorithmForRequest.js
var getChecksumAlgorithmForRequest = (input, { requestChecksumRequired, requestAlgorithmMember, requestChecksumCalculation }) => {
  if (!requestAlgorithmMember) {
    return requestChecksumCalculation === RequestChecksumCalculation.WHEN_SUPPORTED || requestChecksumRequired ? DEFAULT_CHECKSUM_ALGORITHM : void 0;
  }
  if (!input[requestAlgorithmMember]) {
    return void 0;
  }
  const checksumAlgorithm = input[requestAlgorithmMember];
  return checksumAlgorithm;
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/getChecksumLocationName.js
var getChecksumLocationName = (algorithm) => algorithm === ChecksumAlgorithm.MD5 ? "content-md5" : `x-amz-checksum-${algorithm.toLowerCase()}`;

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/hasHeader.js
var hasHeader = (header, headers) => {
  const soughtHeader = header.toLowerCase();
  for (const headerName of Object.keys(headers)) {
    if (soughtHeader === headerName.toLowerCase()) {
      return true;
    }
  }
  return false;
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/hasHeaderWithPrefix.js
var hasHeaderWithPrefix = (headerPrefix, headers) => {
  const soughtHeaderPrefix = headerPrefix.toLowerCase();
  for (const headerName of Object.keys(headers)) {
    if (headerName.toLowerCase().startsWith(soughtHeaderPrefix)) {
      return true;
    }
  }
  return false;
};

// node_modules/@smithy/is-array-buffer/dist-es/index.js
var isArrayBuffer2 = (arg) => typeof ArrayBuffer === "function" && arg instanceof ArrayBuffer || Object.prototype.toString.call(arg) === "[object ArrayBuffer]";

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/isStreaming.js
var isStreaming = (body) => body !== void 0 && typeof body !== "string" && !ArrayBuffer.isView(body) && !isArrayBuffer2(body);

// node_modules/@aws-crypto/crc32c/build/module/aws_crc32c.js
var AwsCrc32c = (
  /** @class */
  function() {
    function AwsCrc32c2() {
      this.crc32c = new Crc32c();
    }
    AwsCrc32c2.prototype.update = function(toHash) {
      if (isEmptyData(toHash))
        return;
      this.crc32c.update(convertToBuffer(toHash));
    };
    AwsCrc32c2.prototype.digest = function() {
      return __awaiter(this, void 0, void 0, function() {
        return __generator(this, function(_a) {
          return [2, numToUint8(this.crc32c.digest())];
        });
      });
    };
    AwsCrc32c2.prototype.reset = function() {
      this.crc32c = new Crc32c();
    };
    return AwsCrc32c2;
  }()
);

// node_modules/@aws-crypto/crc32c/build/module/index.js
var Crc32c = (
  /** @class */
  function() {
    function Crc32c2() {
      this.checksum = 4294967295;
    }
    Crc32c2.prototype.update = function(data2) {
      var e_1, _a;
      try {
        for (var data_1 = __values(data2), data_1_1 = data_1.next(); !data_1_1.done; data_1_1 = data_1.next()) {
          var byte = data_1_1.value;
          this.checksum = this.checksum >>> 8 ^ lookupTable[(this.checksum ^ byte) & 255];
        }
      } catch (e_1_1) {
        e_1 = { error: e_1_1 };
      } finally {
        try {
          if (data_1_1 && !data_1_1.done && (_a = data_1.return)) _a.call(data_1);
        } finally {
          if (e_1) throw e_1.error;
        }
      }
      return this;
    };
    Crc32c2.prototype.digest = function() {
      return (this.checksum ^ 4294967295) >>> 0;
    };
    return Crc32c2;
  }()
);
var a_lookupTable = [
  0,
  4067132163,
  3778769143,
  324072436,
  3348797215,
  904991772,
  648144872,
  3570033899,
  2329499855,
  2024987596,
  1809983544,
  2575936315,
  1296289744,
  3207089363,
  2893594407,
  1578318884,
  274646895,
  3795141740,
  4049975192,
  51262619,
  3619967088,
  632279923,
  922689671,
  3298075524,
  2592579488,
  1760304291,
  2075979607,
  2312596564,
  1562183871,
  2943781820,
  3156637768,
  1313733451,
  549293790,
  3537243613,
  3246849577,
  871202090,
  3878099393,
  357341890,
  102525238,
  4101499445,
  2858735121,
  1477399826,
  1264559846,
  3107202533,
  1845379342,
  2677391885,
  2361733625,
  2125378298,
  820201905,
  3263744690,
  3520608582,
  598981189,
  4151959214,
  85089709,
  373468761,
  3827903834,
  3124367742,
  1213305469,
  1526817161,
  2842354314,
  2107672161,
  2412447074,
  2627466902,
  1861252501,
  1098587580,
  3004210879,
  2688576843,
  1378610760,
  2262928035,
  1955203488,
  1742404180,
  2511436119,
  3416409459,
  969524848,
  714683780,
  3639785095,
  205050476,
  4266873199,
  3976438427,
  526918040,
  1361435347,
  2739821008,
  2954799652,
  1114974503,
  2529119692,
  1691668175,
  2005155131,
  2247081528,
  3690758684,
  697762079,
  986182379,
  3366744552,
  476452099,
  3993867776,
  4250756596,
  255256311,
  1640403810,
  2477592673,
  2164122517,
  1922457750,
  2791048317,
  1412925310,
  1197962378,
  3037525897,
  3944729517,
  427051182,
  170179418,
  4165941337,
  746937522,
  3740196785,
  3451792453,
  1070968646,
  1905808397,
  2213795598,
  2426610938,
  1657317369,
  3053634322,
  1147748369,
  1463399397,
  2773627110,
  4215344322,
  153784257,
  444234805,
  3893493558,
  1021025245,
  3467647198,
  3722505002,
  797665321,
  2197175160,
  1889384571,
  1674398607,
  2443626636,
  1164749927,
  3070701412,
  2757221520,
  1446797203,
  137323447,
  4198817972,
  3910406976,
  461344835,
  3484808360,
  1037989803,
  781091935,
  3705997148,
  2460548119,
  1623424788,
  1939049696,
  2180517859,
  1429367560,
  2807687179,
  3020495871,
  1180866812,
  410100952,
  3927582683,
  4182430767,
  186734380,
  3756733383,
  763408580,
  1053836080,
  3434856499,
  2722870694,
  1344288421,
  1131464017,
  2971354706,
  1708204729,
  2545590714,
  2229949006,
  1988219213,
  680717673,
  3673779818,
  3383336350,
  1002577565,
  4010310262,
  493091189,
  238226049,
  4233660802,
  2987750089,
  1082061258,
  1395524158,
  2705686845,
  1972364758,
  2279892693,
  2494862625,
  1725896226,
  952904198,
  3399985413,
  3656866545,
  731699698,
  4283874585,
  222117402,
  510512622,
  3959836397,
  3280807620,
  837199303,
  582374963,
  3504198960,
  68661723,
  4135334616,
  3844915500,
  390545967,
  1230274059,
  3141532936,
  2825850620,
  1510247935,
  2395924756,
  2091215383,
  1878366691,
  2644384480,
  3553878443,
  565732008,
  854102364,
  3229815391,
  340358836,
  3861050807,
  4117890627,
  119113024,
  1493875044,
  2875275879,
  3090270611,
  1247431312,
  2660249211,
  1828433272,
  2141937292,
  2378227087,
  3811616794,
  291187481,
  34330861,
  4032846830,
  615137029,
  3603020806,
  3314634738,
  939183345,
  1776939221,
  2609017814,
  2295496738,
  2058945313,
  2926798794,
  1545135305,
  1330124605,
  3173225534,
  4084100981,
  17165430,
  307568514,
  3762199681,
  888469610,
  3332340585,
  3587147933,
  665062302,
  2042050490,
  2346497209,
  2559330125,
  1793573966,
  3190661285,
  1279665062,
  1595330642,
  2910671697
];
var lookupTable = uint32ArrayFrom(a_lookupTable);

// node_modules/@aws-sdk/crc64-nvme/dist-es/Crc64Nvme.js
var generateCRC64NVMETable = () => {
  const sliceLength = 8;
  const tables = new Array(sliceLength);
  for (let slice = 0; slice < sliceLength; slice++) {
    const table = new Array(512);
    for (let i2 = 0; i2 < 256; i2++) {
      let crc = BigInt(i2);
      for (let j2 = 0; j2 < 8 * (slice + 1); j2++) {
        if (crc & 1n) {
          crc = crc >> 1n ^ 0x9a6c9329ac4bc9b5n;
        } else {
          crc = crc >> 1n;
        }
      }
      table[i2 * 2] = Number(crc >> 32n & 0xffffffffn);
      table[i2 * 2 + 1] = Number(crc & 0xffffffffn);
    }
    tables[slice] = new Uint32Array(table);
  }
  return tables;
};
var CRC64_NVME_REVERSED_TABLE;
var t0;
var t1;
var t2;
var t3;
var t4;
var t5;
var t6;
var t7;
var ensureTablesInitialized = () => {
  if (!CRC64_NVME_REVERSED_TABLE) {
    CRC64_NVME_REVERSED_TABLE = generateCRC64NVMETable();
    [t0, t1, t2, t3, t4, t5, t6, t7] = CRC64_NVME_REVERSED_TABLE;
  }
};
var Crc64Nvme = class {
  c1 = 0;
  c2 = 0;
  constructor() {
    ensureTablesInitialized();
    this.reset();
  }
  update(data2) {
    const len = data2.length;
    let i2 = 0;
    let crc1 = this.c1;
    let crc2 = this.c2;
    while (i2 + 8 <= len) {
      const idx0 = ((crc2 ^ data2[i2++]) & 255) << 1;
      const idx1 = ((crc2 >>> 8 ^ data2[i2++]) & 255) << 1;
      const idx2 = ((crc2 >>> 16 ^ data2[i2++]) & 255) << 1;
      const idx3 = ((crc2 >>> 24 ^ data2[i2++]) & 255) << 1;
      const idx4 = ((crc1 ^ data2[i2++]) & 255) << 1;
      const idx5 = ((crc1 >>> 8 ^ data2[i2++]) & 255) << 1;
      const idx6 = ((crc1 >>> 16 ^ data2[i2++]) & 255) << 1;
      const idx7 = ((crc1 >>> 24 ^ data2[i2++]) & 255) << 1;
      crc1 = t7[idx0] ^ t6[idx1] ^ t5[idx2] ^ t4[idx3] ^ t3[idx4] ^ t2[idx5] ^ t1[idx6] ^ t0[idx7];
      crc2 = t7[idx0 + 1] ^ t6[idx1 + 1] ^ t5[idx2 + 1] ^ t4[idx3 + 1] ^ t3[idx4 + 1] ^ t2[idx5 + 1] ^ t1[idx6 + 1] ^ t0[idx7 + 1];
    }
    while (i2 < len) {
      const idx = ((crc2 ^ data2[i2]) & 255) << 1;
      crc2 = (crc2 >>> 8 | (crc1 & 255) << 24) >>> 0;
      crc1 = crc1 >>> 8 ^ t0[idx];
      crc2 ^= t0[idx + 1];
      i2++;
    }
    this.c1 = crc1;
    this.c2 = crc2;
  }
  digest() {
    return __async(this, null, function* () {
      const c1 = this.c1 ^ 4294967295;
      const c2 = this.c2 ^ 4294967295;
      return new Uint8Array([
        c1 >>> 24,
        c1 >>> 16 & 255,
        c1 >>> 8 & 255,
        c1 & 255,
        c2 >>> 24,
        c2 >>> 16 & 255,
        c2 >>> 8 & 255,
        c2 & 255
      ]);
    });
  }
  reset() {
    this.c1 = 4294967295;
    this.c2 = 4294967295;
  }
};

// node_modules/@aws-sdk/crc64-nvme/dist-es/crc64-nvme-crt-container.js
var crc64NvmeCrtContainer = {
  CrtCrc64Nvme: null
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/getCrc32ChecksumAlgorithmFunction.browser.js
var getCrc32ChecksumAlgorithmFunction = () => AwsCrc32;

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/types.js
var CLIENT_SUPPORTED_ALGORITHMS = [
  ChecksumAlgorithm.CRC32,
  ChecksumAlgorithm.CRC32C,
  ChecksumAlgorithm.CRC64NVME,
  ChecksumAlgorithm.SHA1,
  ChecksumAlgorithm.SHA256
];
var PRIORITY_ORDER_ALGORITHMS = [
  ChecksumAlgorithm.SHA256,
  ChecksumAlgorithm.SHA1,
  ChecksumAlgorithm.CRC32,
  ChecksumAlgorithm.CRC32C,
  ChecksumAlgorithm.CRC64NVME
];

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/selectChecksumAlgorithmFunction.js
var selectChecksumAlgorithmFunction = (checksumAlgorithm, config) => {
  const { checksumAlgorithms = {} } = config;
  switch (checksumAlgorithm) {
    case ChecksumAlgorithm.MD5:
      return checksumAlgorithms?.MD5 ?? config.md5;
    case ChecksumAlgorithm.CRC32:
      return checksumAlgorithms?.CRC32 ?? getCrc32ChecksumAlgorithmFunction();
    case ChecksumAlgorithm.CRC32C:
      return checksumAlgorithms?.CRC32C ?? AwsCrc32c;
    case ChecksumAlgorithm.CRC64NVME:
      if (typeof crc64NvmeCrtContainer.CrtCrc64Nvme !== "function") {
        return checksumAlgorithms?.CRC64NVME ?? Crc64Nvme;
      }
      return checksumAlgorithms?.CRC64NVME ?? crc64NvmeCrtContainer.CrtCrc64Nvme;
    case ChecksumAlgorithm.SHA1:
      return checksumAlgorithms?.SHA1 ?? config.sha1;
    case ChecksumAlgorithm.SHA256:
      return checksumAlgorithms?.SHA256 ?? config.sha256;
    default:
      if (checksumAlgorithms?.[checksumAlgorithm]) {
        return checksumAlgorithms[checksumAlgorithm];
      }
      throw new Error(`The checksum algorithm "${checksumAlgorithm}" is not supported by the client. Select one of ${CLIENT_SUPPORTED_ALGORITHMS}, or provide an implementation to  the client constructor checksums field.`);
  }
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/stringHasher.js
var stringHasher = (checksumAlgorithmFn, body) => {
  const hash = new checksumAlgorithmFn();
  hash.update(toUint8Array(body || ""));
  return hash.digest();
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/flexibleChecksumsMiddleware.js
var flexibleChecksumsMiddlewareOptions = {
  name: "flexibleChecksumsMiddleware",
  step: "build",
  tags: ["BODY_CHECKSUM"],
  override: true
};
var flexibleChecksumsMiddleware = (config, middlewareConfig) => (next, context) => (args) => __async(void 0, null, function* () {
  if (!HttpRequest.isInstance(args.request)) {
    return next(args);
  }
  if (hasHeaderWithPrefix("x-amz-checksum-", args.request.headers)) {
    return next(args);
  }
  const { request, input } = args;
  const { body: requestBody, headers } = request;
  const { base64Encoder, streamHasher } = config;
  const { requestChecksumRequired, requestAlgorithmMember } = middlewareConfig;
  const requestChecksumCalculation = yield config.requestChecksumCalculation();
  const requestAlgorithmMemberName = requestAlgorithmMember?.name;
  const requestAlgorithmMemberHttpHeader = requestAlgorithmMember?.httpHeader;
  if (requestAlgorithmMemberName && !input[requestAlgorithmMemberName]) {
    if (requestChecksumCalculation === RequestChecksumCalculation.WHEN_SUPPORTED || requestChecksumRequired) {
      input[requestAlgorithmMemberName] = DEFAULT_CHECKSUM_ALGORITHM;
      if (requestAlgorithmMemberHttpHeader) {
        headers[requestAlgorithmMemberHttpHeader] = DEFAULT_CHECKSUM_ALGORITHM;
      }
    }
  }
  const checksumAlgorithm = getChecksumAlgorithmForRequest(input, {
    requestChecksumRequired,
    requestAlgorithmMember: requestAlgorithmMember?.name,
    requestChecksumCalculation
  });
  let updatedBody = requestBody;
  let updatedHeaders = headers;
  if (checksumAlgorithm) {
    switch (checksumAlgorithm) {
      case ChecksumAlgorithm.CRC32:
        setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_CRC32", "U");
        break;
      case ChecksumAlgorithm.CRC32C:
        setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_CRC32C", "V");
        break;
      case ChecksumAlgorithm.CRC64NVME:
        setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_CRC64", "W");
        break;
      case ChecksumAlgorithm.SHA1:
        setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_SHA1", "X");
        break;
      case ChecksumAlgorithm.SHA256:
        setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_SHA256", "Y");
        break;
    }
    const checksumLocationName = getChecksumLocationName(checksumAlgorithm);
    const checksumAlgorithmFn = selectChecksumAlgorithmFunction(checksumAlgorithm, config);
    if (isStreaming(requestBody)) {
      const { getAwsChunkedEncodingStream: getAwsChunkedEncodingStream2, bodyLengthChecker } = config;
      updatedBody = getAwsChunkedEncodingStream2(typeof config.requestStreamBufferSize === "number" && config.requestStreamBufferSize >= 8 * 1024 ? createBufferedReadable(requestBody, config.requestStreamBufferSize, context.logger) : requestBody, {
        base64Encoder,
        bodyLengthChecker,
        checksumLocationName,
        checksumAlgorithmFn,
        streamHasher
      });
      updatedHeaders = __spreadProps(__spreadValues({}, headers), {
        "content-encoding": headers["content-encoding"] ? `${headers["content-encoding"]},aws-chunked` : "aws-chunked",
        "transfer-encoding": "chunked",
        "x-amz-decoded-content-length": headers["content-length"],
        "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        "x-amz-trailer": checksumLocationName
      });
      delete updatedHeaders["content-length"];
    } else if (!hasHeader(checksumLocationName, headers)) {
      const rawChecksum = yield stringHasher(checksumAlgorithmFn, requestBody);
      updatedHeaders = __spreadProps(__spreadValues({}, headers), {
        [checksumLocationName]: base64Encoder(rawChecksum)
      });
    }
  }
  try {
    const result = yield next(__spreadProps(__spreadValues({}, args), {
      request: __spreadProps(__spreadValues({}, request), {
        headers: updatedHeaders,
        body: updatedBody
      })
    }));
    return result;
  } catch (e2) {
    if (e2 instanceof Error && e2.name === "InvalidChunkSizeError") {
      try {
        if (!e2.message.endsWith(".")) {
          e2.message += ".";
        }
        e2.message += " Set [requestStreamBufferSize=number e.g. 65_536] in client constructor to instruct AWS SDK to buffer your input stream.";
      } catch (ignored) {
      }
    }
    throw e2;
  }
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/flexibleChecksumsInputMiddleware.js
var flexibleChecksumsInputMiddlewareOptions = {
  name: "flexibleChecksumsInputMiddleware",
  toMiddleware: "serializerMiddleware",
  relation: "before",
  tags: ["BODY_CHECKSUM"],
  override: true
};
var flexibleChecksumsInputMiddleware = (config, middlewareConfig) => (next, context) => (args) => __async(void 0, null, function* () {
  const input = args.input;
  const { requestValidationModeMember } = middlewareConfig;
  const requestChecksumCalculation = yield config.requestChecksumCalculation();
  const responseChecksumValidation = yield config.responseChecksumValidation();
  switch (requestChecksumCalculation) {
    case RequestChecksumCalculation.WHEN_REQUIRED:
      setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_WHEN_REQUIRED", "a");
      break;
    case RequestChecksumCalculation.WHEN_SUPPORTED:
      setFeature(context, "FLEXIBLE_CHECKSUMS_REQ_WHEN_SUPPORTED", "Z");
      break;
  }
  switch (responseChecksumValidation) {
    case ResponseChecksumValidation.WHEN_REQUIRED:
      setFeature(context, "FLEXIBLE_CHECKSUMS_RES_WHEN_REQUIRED", "c");
      break;
    case ResponseChecksumValidation.WHEN_SUPPORTED:
      setFeature(context, "FLEXIBLE_CHECKSUMS_RES_WHEN_SUPPORTED", "b");
      break;
  }
  if (requestValidationModeMember && !input[requestValidationModeMember]) {
    if (responseChecksumValidation === ResponseChecksumValidation.WHEN_SUPPORTED) {
      input[requestValidationModeMember] = "ENABLED";
    }
  }
  return next(args);
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/getChecksumAlgorithmListForResponse.js
var getChecksumAlgorithmListForResponse = (responseAlgorithms = []) => {
  const validChecksumAlgorithms = [];
  let i2 = PRIORITY_ORDER_ALGORITHMS.length;
  for (const algorithm of responseAlgorithms) {
    const priority = PRIORITY_ORDER_ALGORITHMS.indexOf(algorithm);
    if (priority !== -1) {
      validChecksumAlgorithms[priority] = algorithm;
    } else {
      validChecksumAlgorithms[i2++] = algorithm;
    }
  }
  return validChecksumAlgorithms.filter(Boolean);
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/isChecksumWithPartNumber.js
var isChecksumWithPartNumber = (checksum) => {
  const lastHyphenIndex = checksum.lastIndexOf("-");
  if (lastHyphenIndex !== -1) {
    const numberPart = checksum.slice(lastHyphenIndex + 1);
    if (!numberPart.startsWith("0")) {
      const number = parseInt(numberPart, 10);
      if (!isNaN(number) && number >= 1 && number <= 1e4) {
        return true;
      }
    }
  }
  return false;
};

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/getChecksum.js
var getChecksum = (_0, _1) => __async(void 0, [_0, _1], function* (body, { checksumAlgorithmFn, base64Encoder }) {
  return base64Encoder(yield stringHasher(checksumAlgorithmFn, body));
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/validateChecksumFromResponse.js
var validateChecksumFromResponse = (_0, _1) => __async(void 0, [_0, _1], function* (response, { config, responseAlgorithms, logger: logger2 }) {
  const checksumAlgorithms = getChecksumAlgorithmListForResponse(responseAlgorithms);
  const { body: responseBody, headers: responseHeaders } = response;
  for (const algorithm of checksumAlgorithms) {
    const responseHeader = getChecksumLocationName(algorithm);
    const checksumFromResponse = responseHeaders[responseHeader];
    if (checksumFromResponse) {
      let checksumAlgorithmFn;
      try {
        checksumAlgorithmFn = selectChecksumAlgorithmFunction(algorithm, config);
      } catch (error) {
        if (algorithm === ChecksumAlgorithm.CRC64NVME) {
          logger2?.warn(`Skipping ${ChecksumAlgorithm.CRC64NVME} checksum validation: ${error.message}`);
          continue;
        }
        throw error;
      }
      const { base64Encoder } = config;
      if (isStreaming(responseBody)) {
        response.body = createChecksumStream({
          expectedChecksum: checksumFromResponse,
          checksumSourceLocation: responseHeader,
          checksum: new checksumAlgorithmFn(),
          source: responseBody,
          base64Encoder
        });
        return;
      }
      const checksum = yield getChecksum(responseBody, { checksumAlgorithmFn, base64Encoder });
      if (checksum === checksumFromResponse) {
        break;
      }
      throw new Error(`Checksum mismatch: expected "${checksum}" but received "${checksumFromResponse}" in response header "${responseHeader}".`);
    }
  }
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/flexibleChecksumsResponseMiddleware.js
var flexibleChecksumsResponseMiddlewareOptions = {
  name: "flexibleChecksumsResponseMiddleware",
  toMiddleware: "deserializerMiddleware",
  relation: "after",
  tags: ["BODY_CHECKSUM"],
  override: true
};
var flexibleChecksumsResponseMiddleware = (config, middlewareConfig) => (next, context) => (args) => __async(void 0, null, function* () {
  if (!HttpRequest.isInstance(args.request)) {
    return next(args);
  }
  const input = args.input;
  const result = yield next(args);
  const response = result.response;
  const { requestValidationModeMember, responseAlgorithms } = middlewareConfig;
  if (requestValidationModeMember && input[requestValidationModeMember] === "ENABLED") {
    const { clientName, commandName } = context;
    const customChecksumAlgorithms = Object.keys(config.checksumAlgorithms ?? {}).filter((algorithm) => {
      const responseHeader = getChecksumLocationName(algorithm);
      return response.headers[responseHeader] !== void 0;
    });
    const algoList = getChecksumAlgorithmListForResponse([
      ...responseAlgorithms ?? [],
      ...customChecksumAlgorithms
    ]);
    const isS3WholeObjectMultipartGetResponseChecksum = clientName === "S3Client" && commandName === "GetObjectCommand" && algoList.every((algorithm) => {
      const responseHeader = getChecksumLocationName(algorithm);
      const checksumFromResponse = response.headers[responseHeader];
      return !checksumFromResponse || isChecksumWithPartNumber(checksumFromResponse);
    });
    if (isS3WholeObjectMultipartGetResponseChecksum) {
      return result;
    }
    yield validateChecksumFromResponse(response, {
      config,
      responseAlgorithms: algoList,
      logger: context.logger
    });
  }
  return result;
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/getFlexibleChecksumsPlugin.js
var getFlexibleChecksumsPlugin = (config, middlewareConfig) => ({
  applyToStack: (clientStack) => {
    clientStack.add(flexibleChecksumsMiddleware(config, middlewareConfig), flexibleChecksumsMiddlewareOptions);
    clientStack.addRelativeTo(flexibleChecksumsInputMiddleware(config, middlewareConfig), flexibleChecksumsInputMiddlewareOptions);
    clientStack.addRelativeTo(flexibleChecksumsResponseMiddleware(config, middlewareConfig), flexibleChecksumsResponseMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-flexible-checksums/dist-es/resolveFlexibleChecksumsConfig.js
var resolveFlexibleChecksumsConfig = (input) => {
  const { requestChecksumCalculation, responseChecksumValidation, requestStreamBufferSize } = input;
  return Object.assign(input, {
    requestChecksumCalculation: normalizeProvider(requestChecksumCalculation ?? DEFAULT_REQUEST_CHECKSUM_CALCULATION),
    responseChecksumValidation: normalizeProvider(responseChecksumValidation ?? DEFAULT_RESPONSE_CHECKSUM_VALIDATION),
    requestStreamBufferSize: Number(requestStreamBufferSize ?? 0),
    checksumAlgorithms: input.checksumAlgorithms ?? {}
  });
};

// node_modules/@aws-sdk/middleware-host-header/dist-es/index.js
function resolveHostHeaderConfig(input) {
  return input;
}
var hostHeaderMiddleware = (options) => (next) => (args) => __async(void 0, null, function* () {
  if (!HttpRequest.isInstance(args.request))
    return next(args);
  const { request } = args;
  const { handlerProtocol = "" } = options.requestHandler.metadata || {};
  if (handlerProtocol.indexOf("h2") >= 0 && !request.headers[":authority"]) {
    delete request.headers["host"];
    request.headers[":authority"] = request.hostname + (request.port ? ":" + request.port : "");
  } else if (!request.headers["host"]) {
    let host = request.hostname;
    if (request.port != null)
      host += `:${request.port}`;
    request.headers["host"] = host;
  }
  return next(args);
});
var hostHeaderMiddlewareOptions = {
  name: "hostHeaderMiddleware",
  step: "build",
  priority: "low",
  tags: ["HOST"],
  override: true
};
var getHostHeaderPlugin = (options) => ({
  applyToStack: (clientStack) => {
    clientStack.add(hostHeaderMiddleware(options), hostHeaderMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-logger/dist-es/loggerMiddleware.js
var loggerMiddleware = () => (next, context) => (args) => __async(void 0, null, function* () {
  try {
    const response = yield next(args);
    const { clientName, commandName, logger: logger2, dynamoDbDocumentClientOptions = {} } = context;
    const { overrideInputFilterSensitiveLog, overrideOutputFilterSensitiveLog } = dynamoDbDocumentClientOptions;
    const inputFilterSensitiveLog = overrideInputFilterSensitiveLog ?? context.inputFilterSensitiveLog;
    const outputFilterSensitiveLog = overrideOutputFilterSensitiveLog ?? context.outputFilterSensitiveLog;
    const _a = response.output, { $metadata } = _a, outputWithoutMetadata = __objRest(_a, ["$metadata"]);
    logger2?.info?.({
      clientName,
      commandName,
      input: inputFilterSensitiveLog(args.input),
      output: outputFilterSensitiveLog(outputWithoutMetadata),
      metadata: $metadata
    });
    return response;
  } catch (error) {
    const { clientName, commandName, logger: logger2, dynamoDbDocumentClientOptions = {} } = context;
    const { overrideInputFilterSensitiveLog } = dynamoDbDocumentClientOptions;
    const inputFilterSensitiveLog = overrideInputFilterSensitiveLog ?? context.inputFilterSensitiveLog;
    logger2?.error?.({
      clientName,
      commandName,
      input: inputFilterSensitiveLog(args.input),
      error,
      metadata: error.$metadata
    });
    throw error;
  }
});
var loggerMiddlewareOptions = {
  name: "loggerMiddleware",
  tags: ["LOGGER"],
  step: "initialize",
  override: true
};
var getLoggerPlugin = (options) => ({
  applyToStack: (clientStack) => {
    clientStack.add(loggerMiddleware(), loggerMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-recursion-detection/dist-es/configuration.js
var recursionDetectionMiddlewareOptions = {
  step: "build",
  tags: ["RECURSION_DETECTION"],
  name: "recursionDetectionMiddleware",
  override: true,
  priority: "low"
};

// node_modules/@aws-sdk/middleware-recursion-detection/dist-es/recursionDetectionMiddleware.browser.js
var recursionDetectionMiddleware = () => (next) => (args) => __async(void 0, null, function* () {
  return next(args);
});

// node_modules/@aws-sdk/middleware-recursion-detection/dist-es/getRecursionDetectionPlugin.js
var getRecursionDetectionPlugin = (options) => ({
  applyToStack: (clientStack) => {
    clientStack.add(recursionDetectionMiddleware(), recursionDetectionMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/check-content-length-header.js
var CONTENT_LENGTH_HEADER = "content-length";
var DECODED_CONTENT_LENGTH_HEADER = "x-amz-decoded-content-length";
function checkContentLengthHeader() {
  return (next, context) => (args) => __async(this, null, function* () {
    const { request } = args;
    if (HttpRequest.isInstance(request)) {
      if (!(CONTENT_LENGTH_HEADER in request.headers) && !(DECODED_CONTENT_LENGTH_HEADER in request.headers)) {
        const message = `Are you using a Stream of unknown length as the Body of a PutObject request? Consider using Upload instead from @aws-sdk/lib-storage.`;
        if (typeof context?.logger?.warn === "function" && !(context.logger instanceof NoOpLogger)) {
          context.logger.warn(message);
        } else {
          console.warn(message);
        }
      }
    }
    return next(__spreadValues({}, args));
  });
}
var checkContentLengthHeaderMiddlewareOptions = {
  step: "finalizeRequest",
  tags: ["CHECK_CONTENT_LENGTH_HEADER"],
  name: "getCheckContentLengthHeaderPlugin",
  override: true
};
var getCheckContentLengthHeaderPlugin = (unused) => ({
  applyToStack: (clientStack) => {
    clientStack.add(checkContentLengthHeader(), checkContentLengthHeaderMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/region-redirect-endpoint-middleware.js
var regionRedirectEndpointMiddleware = (config) => {
  return (next, context) => (args) => __async(void 0, null, function* () {
    const originalRegion = yield config.region();
    const regionProviderRef = config.region;
    let unlock = () => {
    };
    if (context.__s3RegionRedirect) {
      Object.defineProperty(config, "region", {
        writable: false,
        value: () => __async(void 0, null, function* () {
          return context.__s3RegionRedirect;
        })
      });
      unlock = () => Object.defineProperty(config, "region", {
        writable: true,
        value: regionProviderRef
      });
    }
    try {
      const result = yield next(args);
      if (context.__s3RegionRedirect) {
        unlock();
        const region = yield config.region();
        if (originalRegion !== region) {
          throw new Error("Region was not restored following S3 region redirect.");
        }
      }
      return result;
    } catch (e2) {
      unlock();
      throw e2;
    }
  });
};
var regionRedirectEndpointMiddlewareOptions = {
  tags: ["REGION_REDIRECT", "S3"],
  name: "regionRedirectEndpointMiddleware",
  override: true,
  relation: "before",
  toMiddleware: "endpointV2Middleware"
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/region-redirect-middleware.js
function regionRedirectMiddleware(clientConfig) {
  return (next, context) => (args) => __async(this, null, function* () {
    try {
      return yield next(args);
    } catch (err) {
      if (clientConfig.followRegionRedirects) {
        const statusCode = err?.$metadata?.httpStatusCode;
        const isHeadBucket = context.commandName === "HeadBucketCommand";
        const bucketRegionHeader = err?.$response?.headers?.["x-amz-bucket-region"];
        if (bucketRegionHeader) {
          if (statusCode === 301 || statusCode === 400 && (err?.name === "IllegalLocationConstraintException" || isHeadBucket)) {
            try {
              const actualRegion = bucketRegionHeader;
              context.logger?.debug(`Redirecting from ${yield clientConfig.region()} to ${actualRegion}`);
              context.__s3RegionRedirect = actualRegion;
            } catch (e2) {
              throw new Error("Region redirect failed: " + e2);
            }
            return next(args);
          }
        }
      }
      throw err;
    }
  });
}
var regionRedirectMiddlewareOptions = {
  step: "initialize",
  tags: ["REGION_REDIRECT", "S3"],
  name: "regionRedirectMiddleware",
  override: true
};
var getRegionRedirectMiddlewarePlugin = (clientConfig) => ({
  applyToStack: (clientStack) => {
    clientStack.add(regionRedirectMiddleware(clientConfig), regionRedirectMiddlewareOptions);
    clientStack.addRelativeTo(regionRedirectEndpointMiddleware(clientConfig), regionRedirectEndpointMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-expires-middleware.js
var s3ExpiresMiddleware = (config) => {
  return (next, context) => (args) => __async(void 0, null, function* () {
    const result = yield next(args);
    const { response } = result;
    if (HttpResponse.isInstance(response)) {
      if (response.headers.expires) {
        response.headers.expiresstring = response.headers.expires;
        try {
          parseRfc7231DateTime(response.headers.expires);
        } catch (e2) {
          context.logger?.warn(`AWS SDK Warning for ${context.clientName}::${context.commandName} response parsing (${response.headers.expires}): ${e2}`);
          delete response.headers.expires;
        }
      }
    }
    return result;
  });
};
var s3ExpiresMiddlewareOptions = {
  tags: ["S3"],
  name: "s3ExpiresMiddleware",
  override: true,
  relation: "after",
  toMiddleware: "deserializerMiddleware"
};
var getS3ExpiresMiddlewarePlugin = (clientConfig) => ({
  applyToStack: (clientStack) => {
    clientStack.addRelativeTo(s3ExpiresMiddleware(clientConfig), s3ExpiresMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/classes/S3ExpressIdentityCache.js
var S3ExpressIdentityCache = class _S3ExpressIdentityCache {
  data;
  lastPurgeTime = Date.now();
  static EXPIRED_CREDENTIAL_PURGE_INTERVAL_MS = 3e4;
  constructor(data2 = {}) {
    this.data = data2;
  }
  get(key) {
    const entry = this.data[key];
    if (!entry) {
      return;
    }
    return entry;
  }
  set(key, entry) {
    this.data[key] = entry;
    return entry;
  }
  delete(key) {
    delete this.data[key];
  }
  purgeExpired() {
    return __async(this, null, function* () {
      const now = Date.now();
      if (this.lastPurgeTime + _S3ExpressIdentityCache.EXPIRED_CREDENTIAL_PURGE_INTERVAL_MS > now) {
        return;
      }
      for (const key in this.data) {
        const entry = this.data[key];
        if (!entry.isRefreshing) {
          const credential = yield entry.identity;
          if (credential.expiration) {
            if (credential.expiration.getTime() < now) {
              delete this.data[key];
            }
          }
        }
      }
    });
  }
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/classes/S3ExpressIdentityCacheEntry.js
var S3ExpressIdentityCacheEntry = class {
  _identity;
  isRefreshing;
  accessed;
  constructor(_identity, isRefreshing = false, accessed = Date.now()) {
    this._identity = _identity;
    this.isRefreshing = isRefreshing;
    this.accessed = accessed;
  }
  get identity() {
    this.accessed = Date.now();
    return this._identity;
  }
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/classes/S3ExpressIdentityProviderImpl.js
var S3ExpressIdentityProviderImpl = class _S3ExpressIdentityProviderImpl {
  createSessionFn;
  cache;
  static REFRESH_WINDOW_MS = 6e4;
  constructor(createSessionFn, cache2 = new S3ExpressIdentityCache()) {
    this.createSessionFn = createSessionFn;
    this.cache = cache2;
  }
  getS3ExpressIdentity(awsIdentity, identityProperties) {
    return __async(this, null, function* () {
      const key = identityProperties.Bucket;
      const { cache: cache2 } = this;
      const entry = cache2.get(key);
      if (entry) {
        return entry.identity.then((identity) => {
          const isExpired = (identity.expiration?.getTime() ?? 0) < Date.now();
          if (isExpired) {
            return cache2.set(key, new S3ExpressIdentityCacheEntry(this.getIdentity(key))).identity;
          }
          const isExpiringSoon = (identity.expiration?.getTime() ?? 0) < Date.now() + _S3ExpressIdentityProviderImpl.REFRESH_WINDOW_MS;
          if (isExpiringSoon && !entry.isRefreshing) {
            entry.isRefreshing = true;
            this.getIdentity(key).then((id) => {
              cache2.set(key, new S3ExpressIdentityCacheEntry(Promise.resolve(id)));
            });
          }
          return identity;
        });
      }
      return cache2.set(key, new S3ExpressIdentityCacheEntry(this.getIdentity(key))).identity;
    });
  }
  getIdentity(key) {
    return __async(this, null, function* () {
      yield this.cache.purgeExpired().catch((error) => {
        console.warn("Error while clearing expired entries in S3ExpressIdentityCache: \n" + error);
      });
      const session = yield this.createSessionFn(key);
      if (!session.Credentials?.AccessKeyId || !session.Credentials?.SecretAccessKey) {
        throw new Error("s3#createSession response credential missing AccessKeyId or SecretAccessKey.");
      }
      const identity = {
        accessKeyId: session.Credentials.AccessKeyId,
        secretAccessKey: session.Credentials.SecretAccessKey,
        sessionToken: session.Credentials.SessionToken,
        expiration: session.Credentials.Expiration ? new Date(session.Credentials.Expiration) : void 0
      };
      return identity;
    });
  }
};

// node_modules/@smithy/signature-v4/dist-es/HeaderFormatter.js
var HeaderFormatter = class {
  format(headers) {
    const chunks = [];
    for (const headerName of Object.keys(headers)) {
      const bytes = fromUtf82(headerName);
      chunks.push(Uint8Array.from([bytes.byteLength]), bytes, this.formatHeaderValue(headers[headerName]));
    }
    const out = new Uint8Array(chunks.reduce((carry, bytes) => carry + bytes.byteLength, 0));
    let position = 0;
    for (const chunk of chunks) {
      out.set(chunk, position);
      position += chunk.byteLength;
    }
    return out;
  }
  formatHeaderValue(header) {
    switch (header.type) {
      case "boolean":
        return Uint8Array.from([header.value ? 0 : 1]);
      case "byte":
        return Uint8Array.from([2, header.value]);
      case "short":
        const shortView = new DataView(new ArrayBuffer(3));
        shortView.setUint8(0, 3);
        shortView.setInt16(1, header.value, false);
        return new Uint8Array(shortView.buffer);
      case "integer":
        const intView = new DataView(new ArrayBuffer(5));
        intView.setUint8(0, 4);
        intView.setInt32(1, header.value, false);
        return new Uint8Array(intView.buffer);
      case "long":
        const longBytes = new Uint8Array(9);
        longBytes[0] = 5;
        longBytes.set(header.value.bytes, 1);
        return longBytes;
      case "binary":
        const binView = new DataView(new ArrayBuffer(3 + header.value.byteLength));
        binView.setUint8(0, 6);
        binView.setUint16(1, header.value.byteLength, false);
        const binBytes = new Uint8Array(binView.buffer);
        binBytes.set(header.value, 3);
        return binBytes;
      case "string":
        const utf8Bytes = fromUtf82(header.value);
        const strView = new DataView(new ArrayBuffer(3 + utf8Bytes.byteLength));
        strView.setUint8(0, 7);
        strView.setUint16(1, utf8Bytes.byteLength, false);
        const strBytes = new Uint8Array(strView.buffer);
        strBytes.set(utf8Bytes, 3);
        return strBytes;
      case "timestamp":
        const tsBytes = new Uint8Array(9);
        tsBytes[0] = 8;
        tsBytes.set(Int64.fromNumber(header.value.valueOf()).bytes, 1);
        return tsBytes;
      case "uuid":
        if (!UUID_PATTERN.test(header.value)) {
          throw new Error(`Invalid UUID received: ${header.value}`);
        }
        const uuidBytes = new Uint8Array(17);
        uuidBytes[0] = 9;
        uuidBytes.set(fromHex(header.value.replace(/\-/g, "")), 1);
        return uuidBytes;
    }
  }
};
var HEADER_VALUE_TYPE;
(function(HEADER_VALUE_TYPE2) {
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["boolTrue"] = 0] = "boolTrue";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["boolFalse"] = 1] = "boolFalse";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["byte"] = 2] = "byte";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["short"] = 3] = "short";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["integer"] = 4] = "integer";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["long"] = 5] = "long";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["byteArray"] = 6] = "byteArray";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["string"] = 7] = "string";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["timestamp"] = 8] = "timestamp";
  HEADER_VALUE_TYPE2[HEADER_VALUE_TYPE2["uuid"] = 9] = "uuid";
})(HEADER_VALUE_TYPE || (HEADER_VALUE_TYPE = {}));
var UUID_PATTERN = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
var Int64 = class _Int64 {
  bytes;
  constructor(bytes) {
    this.bytes = bytes;
    if (bytes.byteLength !== 8) {
      throw new Error("Int64 buffers must be exactly 8 bytes");
    }
  }
  static fromNumber(number) {
    if (number > 9223372036854776e3 || number < -9223372036854776e3) {
      throw new Error(`${number} is too large (or, if negative, too small) to represent as an Int64`);
    }
    const bytes = new Uint8Array(8);
    for (let i2 = 7, remaining = Math.abs(Math.round(number)); i2 > -1 && remaining > 0; i2--, remaining /= 256) {
      bytes[i2] = remaining;
    }
    if (number < 0) {
      negate(bytes);
    }
    return new _Int64(bytes);
  }
  valueOf() {
    const bytes = this.bytes.slice(0);
    const negative = bytes[0] & 128;
    if (negative) {
      negate(bytes);
    }
    return parseInt(toHex(bytes), 16) * (negative ? -1 : 1);
  }
  toString() {
    return String(this.valueOf());
  }
};
function negate(bytes) {
  for (let i2 = 0; i2 < 8; i2++) {
    bytes[i2] ^= 255;
  }
  for (let i2 = 7; i2 > -1; i2--) {
    bytes[i2]++;
    if (bytes[i2] !== 0)
      break;
  }
}

// node_modules/@smithy/signature-v4/dist-es/constants.js
var ALGORITHM_QUERY_PARAM = "X-Amz-Algorithm";
var CREDENTIAL_QUERY_PARAM = "X-Amz-Credential";
var AMZ_DATE_QUERY_PARAM = "X-Amz-Date";
var SIGNED_HEADERS_QUERY_PARAM = "X-Amz-SignedHeaders";
var EXPIRES_QUERY_PARAM = "X-Amz-Expires";
var SIGNATURE_QUERY_PARAM = "X-Amz-Signature";
var TOKEN_QUERY_PARAM = "X-Amz-Security-Token";
var AUTH_HEADER = "authorization";
var AMZ_DATE_HEADER = AMZ_DATE_QUERY_PARAM.toLowerCase();
var DATE_HEADER = "date";
var GENERATED_HEADERS = [AUTH_HEADER, AMZ_DATE_HEADER, DATE_HEADER];
var SIGNATURE_HEADER = SIGNATURE_QUERY_PARAM.toLowerCase();
var SHA256_HEADER = "x-amz-content-sha256";
var TOKEN_HEADER = TOKEN_QUERY_PARAM.toLowerCase();
var ALWAYS_UNSIGNABLE_HEADERS = {
  authorization: true,
  "cache-control": true,
  connection: true,
  expect: true,
  from: true,
  "keep-alive": true,
  "max-forwards": true,
  pragma: true,
  referer: true,
  te: true,
  trailer: true,
  "transfer-encoding": true,
  upgrade: true,
  "user-agent": true,
  "x-amzn-trace-id": true
};
var PROXY_HEADER_PATTERN = /^proxy-/;
var SEC_HEADER_PATTERN = /^sec-/;
var ALGORITHM_IDENTIFIER = "AWS4-HMAC-SHA256";
var EVENT_ALGORITHM_IDENTIFIER = "AWS4-HMAC-SHA256-PAYLOAD";
var UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
var MAX_CACHE_SIZE = 50;
var KEY_TYPE_IDENTIFIER = "aws4_request";
var MAX_PRESIGNED_TTL = 60 * 60 * 24 * 7;

// node_modules/@smithy/signature-v4/dist-es/getCanonicalQuery.js
var getCanonicalQuery = ({ query = {} }) => {
  const keys = [];
  const serialized = {};
  for (const key of Object.keys(query)) {
    if (key.toLowerCase() === SIGNATURE_HEADER) {
      continue;
    }
    const encodedKey = escapeUri(key);
    keys.push(encodedKey);
    const value = query[key];
    if (typeof value === "string") {
      serialized[encodedKey] = `${encodedKey}=${escapeUri(value)}`;
    } else if (Array.isArray(value)) {
      serialized[encodedKey] = value.slice(0).reduce((encoded, value2) => encoded.concat([`${encodedKey}=${escapeUri(value2)}`]), []).sort().join("&");
    }
  }
  return keys.sort().map((key) => serialized[key]).filter((serialized2) => serialized2).join("&");
};

// node_modules/@smithy/signature-v4/dist-es/utilDate.js
var iso8601 = (time) => toDate(time).toISOString().replace(/\.\d{3}Z$/, "Z");
var toDate = (time) => {
  if (typeof time === "number") {
    return new Date(time * 1e3);
  }
  if (typeof time === "string") {
    if (Number(time)) {
      return new Date(Number(time) * 1e3);
    }
    return new Date(time);
  }
  return time;
};

// node_modules/@smithy/signature-v4/dist-es/SignatureV4Base.js
var SignatureV4Base = class {
  service;
  regionProvider;
  credentialProvider;
  sha256;
  uriEscapePath;
  applyChecksum;
  constructor({ applyChecksum, credentials, region, service, sha256, uriEscapePath = true }) {
    this.service = service;
    this.sha256 = sha256;
    this.uriEscapePath = uriEscapePath;
    this.applyChecksum = typeof applyChecksum === "boolean" ? applyChecksum : true;
    this.regionProvider = normalizeProvider(region);
    this.credentialProvider = normalizeProvider(credentials);
  }
  createCanonicalRequest(request, canonicalHeaders, payloadHash) {
    const sortedHeaders = Object.keys(canonicalHeaders).sort();
    return `${request.method}
${this.getCanonicalPath(request)}
${getCanonicalQuery(request)}
${sortedHeaders.map((name) => `${name}:${canonicalHeaders[name]}`).join("\n")}

${sortedHeaders.join(";")}
${payloadHash}`;
  }
  createStringToSign(longDate, credentialScope, canonicalRequest, algorithmIdentifier) {
    return __async(this, null, function* () {
      const hash = new this.sha256();
      hash.update(toUint8Array(canonicalRequest));
      const hashedRequest = yield hash.digest();
      return `${algorithmIdentifier}
${longDate}
${credentialScope}
${toHex(hashedRequest)}`;
    });
  }
  getCanonicalPath({ path }) {
    if (this.uriEscapePath) {
      const normalizedPathSegments = [];
      for (const pathSegment of path.split("/")) {
        if (pathSegment?.length === 0)
          continue;
        if (pathSegment === ".")
          continue;
        if (pathSegment === "..") {
          normalizedPathSegments.pop();
        } else {
          normalizedPathSegments.push(pathSegment);
        }
      }
      const normalizedPath = `${path?.startsWith("/") ? "/" : ""}${normalizedPathSegments.join("/")}${normalizedPathSegments.length > 0 && path?.endsWith("/") ? "/" : ""}`;
      const doubleEncoded = escapeUri(normalizedPath);
      return doubleEncoded.replace(/%2F/g, "/");
    }
    return path;
  }
  validateResolvedCredentials(credentials) {
    if (typeof credentials !== "object" || typeof credentials.accessKeyId !== "string" || typeof credentials.secretAccessKey !== "string") {
      throw new Error("Resolved credential object is not valid");
    }
  }
  formatDate(now) {
    const longDate = iso8601(now).replace(/[\-:]/g, "");
    return {
      longDate,
      shortDate: longDate.slice(0, 8)
    };
  }
  getCanonicalHeaderList(headers) {
    return Object.keys(headers).sort().join(";");
  }
};

// node_modules/@smithy/signature-v4/dist-es/credentialDerivation.js
var signingKeyCache = {};
var cacheQueue = [];
var createScope = (shortDate, region, service) => `${shortDate}/${region}/${service}/${KEY_TYPE_IDENTIFIER}`;
var getSigningKey = (sha256Constructor, credentials, shortDate, region, service) => __async(void 0, null, function* () {
  const credsHash = yield hmac(sha256Constructor, credentials.secretAccessKey, credentials.accessKeyId);
  const cacheKey = `${shortDate}:${region}:${service}:${toHex(credsHash)}:${credentials.sessionToken}`;
  if (cacheKey in signingKeyCache) {
    return signingKeyCache[cacheKey];
  }
  cacheQueue.push(cacheKey);
  while (cacheQueue.length > MAX_CACHE_SIZE) {
    delete signingKeyCache[cacheQueue.shift()];
  }
  let key = `AWS4${credentials.secretAccessKey}`;
  for (const signable of [shortDate, region, service, KEY_TYPE_IDENTIFIER]) {
    key = yield hmac(sha256Constructor, key, signable);
  }
  return signingKeyCache[cacheKey] = key;
});
var hmac = (ctor, secret, data2) => {
  const hash = new ctor(secret);
  hash.update(toUint8Array(data2));
  return hash.digest();
};

// node_modules/@smithy/signature-v4/dist-es/getCanonicalHeaders.js
var getCanonicalHeaders = ({ headers }, unsignableHeaders, signableHeaders) => {
  const canonical = {};
  for (const headerName of Object.keys(headers).sort()) {
    if (headers[headerName] == void 0) {
      continue;
    }
    const canonicalHeaderName = headerName.toLowerCase();
    if (canonicalHeaderName in ALWAYS_UNSIGNABLE_HEADERS || unsignableHeaders?.has(canonicalHeaderName) || PROXY_HEADER_PATTERN.test(canonicalHeaderName) || SEC_HEADER_PATTERN.test(canonicalHeaderName)) {
      if (!signableHeaders || signableHeaders && !signableHeaders.has(canonicalHeaderName)) {
        continue;
      }
    }
    canonical[canonicalHeaderName] = headers[headerName].trim().replace(/\s+/g, " ");
  }
  return canonical;
};

// node_modules/@smithy/signature-v4/dist-es/getPayloadHash.js
var getPayloadHash = (_0, _1) => __async(void 0, [_0, _1], function* ({ headers, body }, hashConstructor) {
  for (const headerName of Object.keys(headers)) {
    if (headerName.toLowerCase() === SHA256_HEADER) {
      return headers[headerName];
    }
  }
  if (body == void 0) {
    return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  } else if (typeof body === "string" || ArrayBuffer.isView(body) || isArrayBuffer(body)) {
    const hashCtor = new hashConstructor();
    hashCtor.update(toUint8Array(body));
    return toHex(yield hashCtor.digest());
  }
  return UNSIGNED_PAYLOAD;
});

// node_modules/@smithy/signature-v4/dist-es/headerUtil.js
var hasHeader2 = (soughtHeader, headers) => {
  soughtHeader = soughtHeader.toLowerCase();
  for (const headerName of Object.keys(headers)) {
    if (soughtHeader === headerName.toLowerCase()) {
      return true;
    }
  }
  return false;
};

// node_modules/@smithy/signature-v4/dist-es/moveHeadersToQuery.js
var moveHeadersToQuery = (request, options = {}) => {
  const { headers, query = {} } = HttpRequest.clone(request);
  for (const name of Object.keys(headers)) {
    const lname = name.toLowerCase();
    if (lname.slice(0, 6) === "x-amz-" && !options.unhoistableHeaders?.has(lname) || options.hoistableHeaders?.has(lname)) {
      query[name] = headers[name];
      delete headers[name];
    }
  }
  return __spreadProps(__spreadValues({}, request), {
    headers,
    query
  });
};

// node_modules/@smithy/signature-v4/dist-es/prepareRequest.js
var prepareRequest = (request) => {
  request = HttpRequest.clone(request);
  for (const headerName of Object.keys(request.headers)) {
    if (GENERATED_HEADERS.indexOf(headerName.toLowerCase()) > -1) {
      delete request.headers[headerName];
    }
  }
  return request;
};

// node_modules/@smithy/signature-v4/dist-es/SignatureV4.js
var SignatureV4 = class extends SignatureV4Base {
  headerFormatter = new HeaderFormatter();
  constructor({ applyChecksum, credentials, region, service, sha256, uriEscapePath = true }) {
    super({
      applyChecksum,
      credentials,
      region,
      service,
      sha256,
      uriEscapePath
    });
  }
  presign(_0) {
    return __async(this, arguments, function* (originalRequest, options = {}) {
      const { signingDate = /* @__PURE__ */ new Date(), expiresIn = 3600, unsignableHeaders, unhoistableHeaders, signableHeaders, hoistableHeaders, signingRegion, signingService } = options;
      const credentials = yield this.credentialProvider();
      this.validateResolvedCredentials(credentials);
      const region = signingRegion ?? (yield this.regionProvider());
      const { longDate, shortDate } = this.formatDate(signingDate);
      if (expiresIn > MAX_PRESIGNED_TTL) {
        return Promise.reject("Signature version 4 presigned URLs must have an expiration date less than one week in the future");
      }
      const scope = createScope(shortDate, region, signingService ?? this.service);
      const request = moveHeadersToQuery(prepareRequest(originalRequest), { unhoistableHeaders, hoistableHeaders });
      if (credentials.sessionToken) {
        request.query[TOKEN_QUERY_PARAM] = credentials.sessionToken;
      }
      request.query[ALGORITHM_QUERY_PARAM] = ALGORITHM_IDENTIFIER;
      request.query[CREDENTIAL_QUERY_PARAM] = `${credentials.accessKeyId}/${scope}`;
      request.query[AMZ_DATE_QUERY_PARAM] = longDate;
      request.query[EXPIRES_QUERY_PARAM] = expiresIn.toString(10);
      const canonicalHeaders = getCanonicalHeaders(request, unsignableHeaders, signableHeaders);
      request.query[SIGNED_HEADERS_QUERY_PARAM] = this.getCanonicalHeaderList(canonicalHeaders);
      request.query[SIGNATURE_QUERY_PARAM] = yield this.getSignature(longDate, scope, this.getSigningKey(credentials, region, shortDate, signingService), this.createCanonicalRequest(request, canonicalHeaders, yield getPayloadHash(originalRequest, this.sha256)));
      return request;
    });
  }
  sign(toSign, options) {
    return __async(this, null, function* () {
      if (typeof toSign === "string") {
        return this.signString(toSign, options);
      } else if (toSign.headers && toSign.payload) {
        return this.signEvent(toSign, options);
      } else if (toSign.message) {
        return this.signMessage(toSign, options);
      } else {
        return this.signRequest(toSign, options);
      }
    });
  }
  signEvent(_0, _1) {
    return __async(this, arguments, function* ({ headers, payload: payload2 }, { signingDate = /* @__PURE__ */ new Date(), priorSignature, signingRegion, signingService, eventStreamCredentials }) {
      const region = signingRegion ?? (yield this.regionProvider());
      const { shortDate, longDate } = this.formatDate(signingDate);
      const scope = createScope(shortDate, region, signingService ?? this.service);
      const hashedPayload = yield getPayloadHash({ headers: {}, body: payload2 }, this.sha256);
      const hash = new this.sha256();
      hash.update(headers);
      const hashedHeaders = toHex(yield hash.digest());
      const stringToSign = [
        EVENT_ALGORITHM_IDENTIFIER,
        longDate,
        scope,
        priorSignature,
        hashedHeaders,
        hashedPayload
      ].join("\n");
      return this.signString(stringToSign, {
        signingDate,
        signingRegion: region,
        signingService,
        eventStreamCredentials
      });
    });
  }
  signMessage(_0, _1) {
    return __async(this, arguments, function* (signableMessage, { signingDate = /* @__PURE__ */ new Date(), signingRegion, signingService, eventStreamCredentials }) {
      const promise = this.signEvent({
        headers: this.headerFormatter.format(signableMessage.message.headers),
        payload: signableMessage.message.body
      }, {
        signingDate,
        signingRegion,
        signingService,
        priorSignature: signableMessage.priorSignature,
        eventStreamCredentials
      });
      return promise.then((signature) => {
        return { message: signableMessage.message, signature };
      });
    });
  }
  signString(_0) {
    return __async(this, arguments, function* (stringToSign, { signingDate = /* @__PURE__ */ new Date(), signingRegion, signingService, eventStreamCredentials } = {}) {
      const credentials = eventStreamCredentials ?? (yield this.credentialProvider());
      this.validateResolvedCredentials(credentials);
      const region = signingRegion ?? (yield this.regionProvider());
      const { shortDate } = this.formatDate(signingDate);
      const hash = new this.sha256(yield this.getSigningKey(credentials, region, shortDate, signingService));
      hash.update(toUint8Array(stringToSign));
      return toHex(yield hash.digest());
    });
  }
  signRequest(_0) {
    return __async(this, arguments, function* (requestToSign, { signingDate = /* @__PURE__ */ new Date(), signableHeaders, unsignableHeaders, signingRegion, signingService } = {}) {
      const credentials = yield this.credentialProvider();
      this.validateResolvedCredentials(credentials);
      const region = signingRegion ?? (yield this.regionProvider());
      const request = prepareRequest(requestToSign);
      const { longDate, shortDate } = this.formatDate(signingDate);
      const scope = createScope(shortDate, region, signingService ?? this.service);
      request.headers[AMZ_DATE_HEADER] = longDate;
      if (credentials.sessionToken) {
        request.headers[TOKEN_HEADER] = credentials.sessionToken;
      }
      const payloadHash = yield getPayloadHash(request, this.sha256);
      if (!hasHeader2(SHA256_HEADER, request.headers) && this.applyChecksum) {
        request.headers[SHA256_HEADER] = payloadHash;
      }
      const canonicalHeaders = getCanonicalHeaders(request, unsignableHeaders, signableHeaders);
      const signature = yield this.getSignature(longDate, scope, this.getSigningKey(credentials, region, shortDate, signingService), this.createCanonicalRequest(request, canonicalHeaders, payloadHash));
      request.headers[AUTH_HEADER] = `${ALGORITHM_IDENTIFIER} Credential=${credentials.accessKeyId}/${scope}, SignedHeaders=${this.getCanonicalHeaderList(canonicalHeaders)}, Signature=${signature}`;
      return request;
    });
  }
  getSignature(longDate, credentialScope, keyPromise, canonicalRequest) {
    return __async(this, null, function* () {
      const stringToSign = yield this.createStringToSign(longDate, credentialScope, canonicalRequest, ALGORITHM_IDENTIFIER);
      const hash = new this.sha256(yield keyPromise);
      hash.update(toUint8Array(stringToSign));
      return toHex(yield hash.digest());
    });
  }
  getSigningKey(credentials, region, shortDate, service) {
    return getSigningKey(this.sha256, credentials, shortDate, region, service || this.service);
  }
};

// node_modules/@smithy/signature-v4/dist-es/signature-v4a-container.js
var signatureV4aContainer = {
  SignatureV4a: null
};

// node_modules/@smithy/util-config-provider/dist-es/types.js
var SelectorType2;
(function(SelectorType4) {
  SelectorType4["ENV"] = "env";
  SelectorType4["CONFIG"] = "shared config entry";
})(SelectorType2 || (SelectorType2 = {}));

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/constants.js
var S3_EXPRESS_BUCKET_TYPE = "Directory";
var S3_EXPRESS_BACKEND = "S3Express";
var S3_EXPRESS_AUTH_SCHEME = "sigv4-s3express";
var SESSION_TOKEN_QUERY_PARAM = "X-Amz-S3session-Token";
var SESSION_TOKEN_HEADER = SESSION_TOKEN_QUERY_PARAM.toLowerCase();

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/classes/SignatureV4S3Express.js
var SignatureV4S3Express = class extends SignatureV4 {
  signWithCredentials(requestToSign, credentials, options) {
    return __async(this, null, function* () {
      const credentialsWithoutSessionToken = getCredentialsWithoutSessionToken(credentials);
      requestToSign.headers[SESSION_TOKEN_HEADER] = credentials.sessionToken;
      const privateAccess = this;
      setSingleOverride(privateAccess, credentialsWithoutSessionToken);
      return privateAccess.signRequest(requestToSign, options ?? {});
    });
  }
  presignWithCredentials(requestToSign, credentials, options) {
    return __async(this, null, function* () {
      const credentialsWithoutSessionToken = getCredentialsWithoutSessionToken(credentials);
      delete requestToSign.headers[SESSION_TOKEN_HEADER];
      requestToSign.headers[SESSION_TOKEN_QUERY_PARAM] = credentials.sessionToken;
      requestToSign.query = requestToSign.query ?? {};
      requestToSign.query[SESSION_TOKEN_QUERY_PARAM] = credentials.sessionToken;
      const privateAccess = this;
      setSingleOverride(privateAccess, credentialsWithoutSessionToken);
      return this.presign(requestToSign, options);
    });
  }
};
function getCredentialsWithoutSessionToken(credentials) {
  const credentialsWithoutSessionToken = {
    accessKeyId: credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
    expiration: credentials.expiration
  };
  return credentialsWithoutSessionToken;
}
function setSingleOverride(privateAccess, credentialsWithoutSessionToken) {
  const id = setTimeout(() => {
    throw new Error("SignatureV4S3Express credential override was created but not called.");
  }, 10);
  const currentCredentialProvider = privateAccess.credentialProvider;
  const overrideCredentialsProviderOnce = () => {
    clearTimeout(id);
    privateAccess.credentialProvider = currentCredentialProvider;
    return Promise.resolve(credentialsWithoutSessionToken);
  };
  privateAccess.credentialProvider = overrideCredentialsProviderOnce;
}

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/functions/s3ExpressMiddleware.js
var s3ExpressMiddleware = (options) => {
  return (next, context) => (args) => __async(void 0, null, function* () {
    if (context.endpointV2) {
      const endpoint = context.endpointV2;
      const isS3ExpressAuth = endpoint.properties?.authSchemes?.[0]?.name === S3_EXPRESS_AUTH_SCHEME;
      const isS3ExpressBucket = endpoint.properties?.backend === S3_EXPRESS_BACKEND || endpoint.properties?.bucketType === S3_EXPRESS_BUCKET_TYPE;
      if (isS3ExpressBucket) {
        setFeature(context, "S3_EXPRESS_BUCKET", "J");
        context.isS3ExpressBucket = true;
      }
      if (isS3ExpressAuth) {
        const requestBucket = args.input.Bucket;
        if (requestBucket) {
          const s3ExpressIdentity = yield options.s3ExpressIdentityProvider.getS3ExpressIdentity(yield options.credentials(), {
            Bucket: requestBucket
          });
          context.s3ExpressIdentity = s3ExpressIdentity;
          if (HttpRequest.isInstance(args.request) && s3ExpressIdentity.sessionToken) {
            args.request.headers[SESSION_TOKEN_HEADER] = s3ExpressIdentity.sessionToken;
          }
        }
      }
    }
    return next(args);
  });
};
var s3ExpressMiddlewareOptions = {
  name: "s3ExpressMiddleware",
  step: "build",
  tags: ["S3", "S3_EXPRESS"],
  override: true
};
var getS3ExpressPlugin = (options) => ({
  applyToStack: (clientStack) => {
    clientStack.add(s3ExpressMiddleware(options), s3ExpressMiddlewareOptions);
  }
});

// node_modules/@smithy/core/dist-es/middleware-http-auth-scheme/resolveAuthOptions.js
var resolveAuthOptions = (candidateAuthOptions, authSchemePreference) => {
  if (!authSchemePreference || authSchemePreference.length === 0) {
    return candidateAuthOptions;
  }
  const preferredAuthOptions = [];
  for (const preferredSchemeName of authSchemePreference) {
    for (const candidateAuthOption of candidateAuthOptions) {
      const candidateAuthSchemeName = candidateAuthOption.schemeId.split("#")[1];
      if (candidateAuthSchemeName === preferredSchemeName) {
        preferredAuthOptions.push(candidateAuthOption);
      }
    }
  }
  for (const candidateAuthOption of candidateAuthOptions) {
    if (!preferredAuthOptions.find(({ schemeId }) => schemeId === candidateAuthOption.schemeId)) {
      preferredAuthOptions.push(candidateAuthOption);
    }
  }
  return preferredAuthOptions;
};

// node_modules/@smithy/core/dist-es/middleware-http-auth-scheme/httpAuthSchemeMiddleware.js
function convertHttpAuthSchemesToMap(httpAuthSchemes) {
  const map2 = /* @__PURE__ */ new Map();
  for (const scheme of httpAuthSchemes) {
    map2.set(scheme.schemeId, scheme);
  }
  return map2;
}
var httpAuthSchemeMiddleware = (config, mwOptions) => (next, context) => (args) => __async(void 0, null, function* () {
  const options = config.httpAuthSchemeProvider(yield mwOptions.httpAuthSchemeParametersProvider(config, context, args.input));
  const authSchemePreference = config.authSchemePreference ? yield config.authSchemePreference() : [];
  const resolvedOptions = resolveAuthOptions(options, authSchemePreference);
  const authSchemes = convertHttpAuthSchemesToMap(config.httpAuthSchemes);
  const smithyContext = getSmithyContext(context);
  const failureReasons = [];
  for (const option of resolvedOptions) {
    const scheme = authSchemes.get(option.schemeId);
    if (!scheme) {
      failureReasons.push(`HttpAuthScheme \`${option.schemeId}\` was not enabled for this service.`);
      continue;
    }
    const identityProvider = scheme.identityProvider(yield mwOptions.identityProviderConfigProvider(config));
    if (!identityProvider) {
      failureReasons.push(`HttpAuthScheme \`${option.schemeId}\` did not have an IdentityProvider configured.`);
      continue;
    }
    const { identityProperties = {}, signingProperties = {} } = option.propertiesExtractor?.(config, context) || {};
    option.identityProperties = Object.assign(option.identityProperties || {}, identityProperties);
    option.signingProperties = Object.assign(option.signingProperties || {}, signingProperties);
    smithyContext.selectedHttpAuthScheme = {
      httpAuthOption: option,
      identity: yield identityProvider(option.identityProperties),
      signer: scheme.signer
    };
    break;
  }
  if (!smithyContext.selectedHttpAuthScheme) {
    throw new Error(failureReasons.join("\n"));
  }
  return next(args);
});

// node_modules/@smithy/core/dist-es/middleware-http-auth-scheme/getHttpAuthSchemeEndpointRuleSetPlugin.js
var httpAuthSchemeEndpointRuleSetMiddlewareOptions = {
  step: "serialize",
  tags: ["HTTP_AUTH_SCHEME"],
  name: "httpAuthSchemeMiddleware",
  override: true,
  relation: "before",
  toMiddleware: "endpointV2Middleware"
};
var getHttpAuthSchemeEndpointRuleSetPlugin = (config, { httpAuthSchemeParametersProvider, identityProviderConfigProvider }) => ({
  applyToStack: (clientStack) => {
    clientStack.addRelativeTo(httpAuthSchemeMiddleware(config, {
      httpAuthSchemeParametersProvider,
      identityProviderConfigProvider
    }), httpAuthSchemeEndpointRuleSetMiddlewareOptions);
  }
});

// node_modules/@smithy/core/dist-es/middleware-http-signing/httpSigningMiddleware.js
var defaultErrorHandler = (signingProperties) => (error) => {
  throw error;
};
var defaultSuccessHandler = (httpResponse, signingProperties) => {
};
var httpSigningMiddleware = (config) => (next, context) => (args) => __async(void 0, null, function* () {
  if (!HttpRequest.isInstance(args.request)) {
    return next(args);
  }
  const smithyContext = getSmithyContext(context);
  const scheme = smithyContext.selectedHttpAuthScheme;
  if (!scheme) {
    throw new Error(`No HttpAuthScheme was selected: unable to sign request`);
  }
  const { httpAuthOption: { signingProperties = {} }, identity, signer } = scheme;
  const output = yield next(__spreadProps(__spreadValues({}, args), {
    request: yield signer.sign(args.request, identity, signingProperties)
  })).catch((signer.errorHandler || defaultErrorHandler)(signingProperties));
  (signer.successHandler || defaultSuccessHandler)(output.response, signingProperties);
  return output;
});

// node_modules/@smithy/core/dist-es/middleware-http-signing/getHttpSigningMiddleware.js
var httpSigningMiddlewareOptions = {
  step: "finalizeRequest",
  tags: ["HTTP_SIGNING"],
  name: "httpSigningMiddleware",
  aliases: ["apiKeyMiddleware", "tokenMiddleware", "awsAuthMiddleware"],
  override: true,
  relation: "after",
  toMiddleware: "retryMiddleware"
};
var getHttpSigningPlugin = (config) => ({
  applyToStack: (clientStack) => {
    clientStack.addRelativeTo(httpSigningMiddleware(config), httpSigningMiddlewareOptions);
  }
});

// node_modules/@smithy/core/dist-es/normalizeProvider.js
var normalizeProvider2 = (input) => {
  if (typeof input === "function")
    return input;
  const promisified = Promise.resolve(input);
  return () => promisified;
};

// node_modules/@smithy/core/dist-es/pagination/createPaginator.js
var makePagedClientRequest = (CommandCtor, client, input, withCommand = (_) => _, ...args) => __async(void 0, null, function* () {
  let command = new CommandCtor(input);
  command = withCommand(command) ?? command;
  return yield client.send(command, ...args);
});
function createPaginator(ClientCtor, CommandCtor, inputTokenName, outputTokenName, pageSizeTokenName) {
  return function paginateOperation(config, input, ...additionalArguments) {
    return __asyncGenerator(this, null, function* () {
      const _input = input;
      let token = config.startingToken ?? _input[inputTokenName];
      let hasNext = true;
      let page;
      while (hasNext) {
        _input[inputTokenName] = token;
        if (pageSizeTokenName) {
          _input[pageSizeTokenName] = _input[pageSizeTokenName] ?? config.pageSize;
        }
        if (config.client instanceof ClientCtor) {
          page = yield new __await(makePagedClientRequest(CommandCtor, config.client, input, config.withCommand, ...additionalArguments));
        } else {
          throw new Error(`Invalid client, expected instance of ${ClientCtor.name}`);
        }
        yield page;
        const prevToken = token;
        token = get(page, outputTokenName);
        hasNext = !!(token && (!config.stopOnSameToken || token !== prevToken));
      }
      return void 0;
    });
  };
}
var get = (fromObject, path) => {
  let cursor = fromObject;
  const pathComponents = path.split(".");
  for (const step of pathComponents) {
    if (!cursor || typeof cursor !== "object") {
      return void 0;
    }
    cursor = cursor[step];
  }
  return cursor;
};

// node_modules/@smithy/core/dist-es/util-identity-and-auth/DefaultIdentityProviderConfig.js
var DefaultIdentityProviderConfig = class {
  authSchemes = /* @__PURE__ */ new Map();
  constructor(config) {
    for (const key in config) {
      const value = config[key];
      if (value !== void 0) {
        this.authSchemes.set(key, value);
      }
    }
  }
  getIdentityProvider(schemeId) {
    return this.authSchemes.get(schemeId);
  }
};

// node_modules/@smithy/core/dist-es/util-identity-and-auth/memoizeIdentityProvider.js
var createIsIdentityExpiredFunction = (expirationMs) => function isIdentityExpired2(identity) {
  return doesIdentityRequireRefresh(identity) && identity.expiration.getTime() - Date.now() < expirationMs;
};
var EXPIRATION_MS = 3e5;
var isIdentityExpired = createIsIdentityExpiredFunction(EXPIRATION_MS);
var doesIdentityRequireRefresh = (identity) => identity.expiration !== void 0;
var memoizeIdentityProvider = (provider, isExpired, requiresRefresh) => {
  if (provider === void 0) {
    return void 0;
  }
  const normalizedProvider = typeof provider !== "function" ? () => __async(void 0, null, function* () {
    return Promise.resolve(provider);
  }) : provider;
  let resolved;
  let pending;
  let hasResult;
  let isConstant = false;
  const coalesceProvider = (options) => __async(void 0, null, function* () {
    if (!pending) {
      pending = normalizedProvider(options);
    }
    try {
      resolved = yield pending;
      hasResult = true;
      isConstant = false;
    } finally {
      pending = void 0;
    }
    return resolved;
  });
  if (isExpired === void 0) {
    return (options) => __async(void 0, null, function* () {
      if (!hasResult || options?.forceRefresh) {
        resolved = yield coalesceProvider(options);
      }
      return resolved;
    });
  }
  return (options) => __async(void 0, null, function* () {
    if (!hasResult || options?.forceRefresh) {
      resolved = yield coalesceProvider(options);
    }
    if (isConstant) {
      return resolved;
    }
    if (!requiresRefresh(resolved)) {
      isConstant = true;
      return resolved;
    }
    if (isExpired(resolved)) {
      yield coalesceProvider(options);
      return resolved;
    }
    return resolved;
  });
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/functions/signS3Express.js
var signS3Express = (s3ExpressIdentity, signingOptions, request, sigV4MultiRegionSigner) => __async(void 0, null, function* () {
  const signedRequest = yield sigV4MultiRegionSigner.signWithCredentials(request, s3ExpressIdentity, {});
  if (signedRequest.headers["X-Amz-Security-Token"] || signedRequest.headers["x-amz-security-token"]) {
    throw new Error("X-Amz-Security-Token must not be set for s3-express requests.");
  }
  return signedRequest;
});

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3-express/functions/s3ExpressHttpSigningMiddleware.js
var defaultErrorHandler2 = (signingProperties) => (error) => {
  throw error;
};
var defaultSuccessHandler2 = (httpResponse, signingProperties) => {
};
var s3ExpressHttpSigningMiddleware = (config) => (next, context) => (args) => __async(void 0, null, function* () {
  if (!HttpRequest.isInstance(args.request)) {
    return next(args);
  }
  const smithyContext = getSmithyContext(context);
  const scheme = smithyContext.selectedHttpAuthScheme;
  if (!scheme) {
    throw new Error(`No HttpAuthScheme was selected: unable to sign request`);
  }
  const { httpAuthOption: { signingProperties = {} }, identity, signer } = scheme;
  let request;
  if (context.s3ExpressIdentity) {
    request = yield signS3Express(context.s3ExpressIdentity, signingProperties, args.request, yield config.signer());
  } else {
    request = yield signer.sign(args.request, identity, signingProperties);
  }
  const output = yield next(__spreadProps(__spreadValues({}, args), {
    request
  })).catch((signer.errorHandler || defaultErrorHandler2)(signingProperties));
  (signer.successHandler || defaultSuccessHandler2)(output.response, signingProperties);
  return output;
});
var getS3ExpressHttpSigningPlugin = (config) => ({
  applyToStack: (clientStack) => {
    clientStack.addRelativeTo(s3ExpressHttpSigningMiddleware(config), httpSigningMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/s3Configuration.js
var resolveS3Config = (input, { session }) => {
  const [s3ClientProvider, CreateSessionCommandCtor] = session;
  const { forcePathStyle, useAccelerateEndpoint, disableMultiregionAccessPoints, followRegionRedirects, s3ExpressIdentityProvider, bucketEndpoint, expectContinueHeader } = input;
  return Object.assign(input, {
    forcePathStyle: forcePathStyle ?? false,
    useAccelerateEndpoint: useAccelerateEndpoint ?? false,
    disableMultiregionAccessPoints: disableMultiregionAccessPoints ?? false,
    followRegionRedirects: followRegionRedirects ?? false,
    s3ExpressIdentityProvider: s3ExpressIdentityProvider ?? new S3ExpressIdentityProviderImpl((key) => __async(void 0, null, function* () {
      return s3ClientProvider().send(new CreateSessionCommandCtor({
        Bucket: key
      }));
    })),
    bucketEndpoint: bucketEndpoint ?? false,
    expectContinueHeader: expectContinueHeader ?? 2097152
  });
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/toStream.browser.js
function toStream(bytes) {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    }
  });
}

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/throw-200-exceptions.js
var THROW_IF_EMPTY_BODY = {
  CopyObjectCommand: true,
  UploadPartCopyCommand: true,
  CompleteMultipartUploadCommand: true
};
var throw200ExceptionsMiddleware = (config) => (next, context) => (args) => __async(void 0, null, function* () {
  const result = yield next(args);
  const { response } = result;
  if (!HttpResponse.isInstance(response)) {
    return result;
  }
  const { statusCode, body } = response;
  if (statusCode < 200 || statusCode >= 300) {
    return result;
  }
  const bodyBytes = yield collectBody2(body, config);
  response.body = toStream(bodyBytes);
  if (bodyBytes.length === 0 && THROW_IF_EMPTY_BODY[context.commandName]) {
    const err = new Error("S3 aborted request");
    err.$metadata = {
      httpStatusCode: 503
    };
    err.name = "InternalError";
    throw err;
  }
  const bodyStringTail = config.utf8Encoder(bodyBytes.subarray(bodyBytes.length - 16));
  if (bodyStringTail && bodyStringTail.endsWith("</Error>")) {
    response.statusCode = 503;
  }
  return result;
});
var collectBody2 = (streamBody = new Uint8Array(), context) => {
  if (streamBody instanceof Uint8Array) {
    return Promise.resolve(streamBody);
  }
  return context.streamCollector(streamBody) || Promise.resolve(new Uint8Array());
};
var throw200ExceptionsMiddlewareOptions = {
  relation: "after",
  toMiddleware: "deserializerMiddleware",
  tags: ["THROW_200_EXCEPTIONS", "S3"],
  name: "throw200ExceptionsMiddleware",
  override: true
};
var getThrow200ExceptionsPlugin = (config) => ({
  applyToStack: (clientStack) => {
    clientStack.addRelativeTo(throw200ExceptionsMiddleware(config), throw200ExceptionsMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/util-arn-parser/dist-es/index.js
var validate = (str) => typeof str === "string" && str.indexOf("arn:") === 0 && str.split(":").length >= 6;

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/bucket-endpoint-middleware.js
function bucketEndpointMiddleware(options) {
  return (next, context) => (args) => __async(this, null, function* () {
    if (options.bucketEndpoint) {
      const endpoint = context.endpointV2;
      if (endpoint) {
        const bucket = args.input.Bucket;
        if (typeof bucket === "string") {
          try {
            const bucketEndpointUrl = new URL(bucket);
            context.endpointV2 = __spreadProps(__spreadValues({}, endpoint), {
              url: bucketEndpointUrl
            });
          } catch (e2) {
            const warning = `@aws-sdk/middleware-sdk-s3: bucketEndpoint=true was set but Bucket=${bucket} could not be parsed as URL.`;
            if (context.logger?.constructor?.name === "NoOpLogger") {
              console.warn(warning);
            } else {
              context.logger?.warn?.(warning);
            }
            throw e2;
          }
        }
      }
    }
    return next(args);
  });
}
var bucketEndpointMiddlewareOptions = {
  name: "bucketEndpointMiddleware",
  override: true,
  relation: "after",
  toMiddleware: "endpointV2Middleware"
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/validate-bucket-name.js
function validateBucketNameMiddleware({ bucketEndpoint }) {
  return (next) => (args) => __async(this, null, function* () {
    const { input: { Bucket } } = args;
    if (!bucketEndpoint && typeof Bucket === "string" && !validate(Bucket) && Bucket.indexOf("/") >= 0) {
      const err = new Error(`Bucket name shouldn't contain '/', received '${Bucket}'`);
      err.name = "InvalidBucketName";
      throw err;
    }
    return next(__spreadValues({}, args));
  });
}
var validateBucketNameMiddlewareOptions = {
  step: "initialize",
  tags: ["VALIDATE_BUCKET_NAME"],
  name: "validateBucketNameMiddleware",
  override: true
};
var getValidateBucketNamePlugin = (options) => ({
  applyToStack: (clientStack) => {
    clientStack.add(validateBucketNameMiddleware(options), validateBucketNameMiddlewareOptions);
    clientStack.addRelativeTo(bucketEndpointMiddleware(options), bucketEndpointMiddlewareOptions);
  }
});

// node_modules/@smithy/core/dist-es/submodules/cbor/cbor-types.js
var extendedOneByte = 24;
var extendedFloat16 = 25;
var extendedFloat32 = 26;
var extendedFloat64 = 27;
function alloc(size) {
  return typeof Buffer !== "undefined" ? Buffer.alloc(size) : new Uint8Array(size);
}
var tagSymbol = Symbol("@smithy/core/cbor::tagSymbol");

// node_modules/@smithy/core/dist-es/submodules/cbor/cbor-decode.js
var USE_TEXT_DECODER = typeof TextDecoder !== "undefined";
var payload = alloc(0);
var dataView = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
var textDecoder = USE_TEXT_DECODER ? new TextDecoder() : null;
var minorValueToArgumentLength = {
  [extendedOneByte]: 1,
  [extendedFloat16]: 2,
  [extendedFloat32]: 4,
  [extendedFloat64]: 8
};

// node_modules/@smithy/core/dist-es/submodules/cbor/cbor-encode.js
var initialSize = 2048;
var data = alloc(initialSize);
var dataView2 = new DataView(data.buffer, data.byteOffset, data.byteLength);

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/ProtocolLib.js
var ProtocolLib = class {
  queryCompat;
  errorRegistry;
  constructor(queryCompat = false) {
    this.queryCompat = queryCompat;
  }
  resolveRestContentType(defaultContentType, inputSchema) {
    const members = inputSchema.getMemberSchemas();
    const httpPayloadMember = Object.values(members).find((m2) => {
      return !!m2.getMergedTraits().httpPayload;
    });
    if (httpPayloadMember) {
      const mediaType = httpPayloadMember.getMergedTraits().mediaType;
      if (mediaType) {
        return mediaType;
      } else if (httpPayloadMember.isStringSchema()) {
        return "text/plain";
      } else if (httpPayloadMember.isBlobSchema()) {
        return "application/octet-stream";
      } else {
        return defaultContentType;
      }
    } else if (!inputSchema.isUnitSchema()) {
      const hasBody = Object.values(members).find((m2) => {
        const { httpQuery, httpQueryParams, httpHeader, httpLabel, httpPrefixHeaders } = m2.getMergedTraits();
        const noPrefixHeaders = httpPrefixHeaders === void 0;
        return !httpQuery && !httpQueryParams && !httpHeader && !httpLabel && noPrefixHeaders;
      });
      if (hasBody) {
        return defaultContentType;
      }
    }
  }
  getErrorSchemaOrThrowBaseException(errorIdentifier, defaultNamespace, response, dataObject, metadata, getErrorSchema) {
    return __async(this, null, function* () {
      let errorName = errorIdentifier;
      if (errorIdentifier.includes("#")) {
        [, errorName] = errorIdentifier.split("#");
      }
      const errorMetadata = {
        $metadata: metadata,
        $fault: response.statusCode < 500 ? "client" : "server"
      };
      if (!this.errorRegistry) {
        throw new Error("@aws-sdk/core/protocols - error handler not initialized.");
      }
      try {
        const errorSchema = getErrorSchema?.(this.errorRegistry, errorName) ?? this.errorRegistry.getSchema(errorIdentifier);
        return { errorSchema, errorMetadata };
      } catch (e2) {
        dataObject.message = dataObject.message ?? dataObject.Message ?? "UnknownError";
        const synthetic = this.errorRegistry;
        const baseExceptionSchema = synthetic.getBaseException();
        if (baseExceptionSchema) {
          const ErrorCtor = synthetic.getErrorCtor(baseExceptionSchema) ?? Error;
          throw this.decorateServiceException(Object.assign(new ErrorCtor({ name: errorName }), errorMetadata), dataObject);
        }
        const d2 = dataObject;
        const message = d2?.message ?? d2?.Message ?? d2?.Error?.Message ?? d2?.Error?.message;
        throw this.decorateServiceException(Object.assign(new Error(message), {
          name: errorName
        }, errorMetadata), dataObject);
      }
    });
  }
  compose(composite, errorIdentifier, defaultNamespace) {
    let namespace = defaultNamespace;
    if (errorIdentifier.includes("#")) {
      [namespace] = errorIdentifier.split("#");
    }
    const staticRegistry = TypeRegistry.for(namespace);
    const defaultSyntheticRegistry = TypeRegistry.for("smithy.ts.sdk.synthetic." + defaultNamespace);
    composite.copyFrom(staticRegistry);
    composite.copyFrom(defaultSyntheticRegistry);
    this.errorRegistry = composite;
  }
  decorateServiceException(exception, additions = {}) {
    if (this.queryCompat) {
      const msg = exception.Message ?? additions.Message;
      const error = decorateServiceException(exception, additions);
      if (msg) {
        error.message = msg;
      }
      const errorObj = error.Error ?? {};
      errorObj.Type = error.Error?.Type;
      errorObj.Code = error.Error?.Code;
      errorObj.Message = error.Error?.message ?? error.Error?.Message ?? msg;
      error.Error = errorObj;
      const reqId = error.$metadata.requestId;
      if (reqId) {
        error.RequestId = reqId;
      }
      return error;
    }
    return decorateServiceException(exception, additions);
  }
  setQueryCompatError(output, response) {
    const queryErrorHeader = response.headers?.["x-amzn-query-error"];
    if (output !== void 0 && queryErrorHeader != null) {
      const [Code, Type2] = queryErrorHeader.split(";");
      const keys = Object.keys(output);
      const Error2 = {
        Code,
        Type: Type2
      };
      output.Code = Code;
      output.Type = Type2;
      for (let i2 = 0; i2 < keys.length; i2++) {
        const k2 = keys[i2];
        Error2[k2 === "message" ? "Message" : k2] = output[k2];
      }
      delete Error2.__type;
      output.Error = Error2;
    }
  }
  queryCompatOutput(queryCompatErrorData, errorData) {
    if (queryCompatErrorData.Error) {
      errorData.Error = queryCompatErrorData.Error;
    }
    if (queryCompatErrorData.Type) {
      errorData.Type = queryCompatErrorData.Type;
    }
    if (queryCompatErrorData.Code) {
      errorData.Code = queryCompatErrorData.Code;
    }
  }
  findQueryCompatibleError(registry, errorName) {
    try {
      return registry.getSchema(errorName);
    } catch (e2) {
      return registry.find((schema) => NormalizedSchema.of(schema).getMergedTraits().awsQueryError?.[0] === errorName);
    }
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/ConfigurableSerdeContext.js
var SerdeContextConfig = class {
  serdeContext;
  setSerdeContext(serdeContext) {
    this.serdeContext = serdeContext;
  }
};

// node_modules/@aws-sdk/core/node_modules/@smithy/util-base64/dist-es/constants.browser.js
var chars = `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/`;
var alphabetByEncoding = Object.entries(chars).reduce((acc, [i2, c2]) => {
  acc[c2] = Number(i2);
  return acc;
}, {});
var alphabetByValue = chars.split("");
var bitsPerLetter = 6;
var bitsPerByte = 8;
var maxLetterValue = 63;

// node_modules/@aws-sdk/core/node_modules/@smithy/util-base64/dist-es/fromBase64.browser.js
var fromBase642 = (input) => {
  let totalByteLength = input.length / 4 * 3;
  if (input.slice(-2) === "==") {
    totalByteLength -= 2;
  } else if (input.slice(-1) === "=") {
    totalByteLength--;
  }
  const out = new ArrayBuffer(totalByteLength);
  const dataView3 = new DataView(out);
  for (let i2 = 0; i2 < input.length; i2 += 4) {
    let bits = 0;
    let bitLength = 0;
    for (let j2 = i2, limit = i2 + 3; j2 <= limit; j2++) {
      if (input[j2] !== "=") {
        if (!(input[j2] in alphabetByEncoding)) {
          throw new TypeError(`Invalid character ${input[j2]} in base64 string.`);
        }
        bits |= alphabetByEncoding[input[j2]] << (limit - j2) * bitsPerLetter;
        bitLength += bitsPerLetter;
      } else {
        bits >>= bitsPerLetter;
      }
    }
    const chunkOffset = i2 / 4 * 3;
    bits >>= bitLength % bitsPerByte;
    const byteLength = Math.floor(bitLength / bitsPerByte);
    for (let k2 = 0; k2 < byteLength; k2++) {
      const offset = (byteLength - k2 - 1) * bitsPerByte;
      dataView3.setUint8(chunkOffset + k2, (bits & 255 << offset) >> offset);
    }
  }
  return new Uint8Array(out);
};

// node_modules/@aws-sdk/core/node_modules/@smithy/util-utf8/dist-es/fromUtf8.browser.js
var fromUtf83 = (input) => new TextEncoder().encode(input);

// node_modules/@aws-sdk/core/node_modules/@smithy/util-utf8/dist-es/toUtf8.browser.js
var toUtf82 = (input) => {
  if (typeof input === "string") {
    return input;
  }
  if (typeof input !== "object" || typeof input.byteOffset !== "number" || typeof input.byteLength !== "number") {
    throw new Error("@smithy/util-utf8: toUtf8 encoder function only accepts string | Uint8Array.");
  }
  return new TextDecoder("utf-8").decode(input);
};

// node_modules/@aws-sdk/core/node_modules/@smithy/util-base64/dist-es/toBase64.browser.js
function toBase642(_input) {
  let input;
  if (typeof _input === "string") {
    input = fromUtf83(_input);
  } else {
    input = _input;
  }
  const isArrayLike = typeof input === "object" && typeof input.length === "number";
  const isUint8Array = typeof input === "object" && typeof input.byteOffset === "number" && typeof input.byteLength === "number";
  if (!isArrayLike && !isUint8Array) {
    throw new Error("@smithy/util-base64: toBase64 encoder function only accepts string | Uint8Array.");
  }
  let str = "";
  for (let i2 = 0; i2 < input.length; i2 += 3) {
    let bits = 0;
    let bitLength = 0;
    for (let j2 = i2, limit = Math.min(i2 + 3, input.length); j2 < limit; j2++) {
      bits |= input[j2] << (limit - j2 - 1) * bitsPerByte;
      bitLength += bitsPerByte;
    }
    const bitClusterCount = Math.ceil(bitLength / bitsPerLetter);
    bits <<= bitClusterCount * bitsPerLetter - bitLength;
    for (let k2 = 1; k2 <= bitClusterCount; k2++) {
      const offset = (bitClusterCount - k2) * bitsPerLetter;
      str += alphabetByValue[(bits & maxLetterValue << offset) >> offset];
    }
    str += "==".slice(0, 4 - bitClusterCount);
  }
  return str;
}

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/UnionSerde.js
var UnionSerde = class {
  from;
  to;
  keys;
  constructor(from, to) {
    this.from = from;
    this.to = to;
    const keys = Object.keys(this.from);
    const set = new Set(keys);
    set.delete("__type");
    this.keys = set;
  }
  mark(key) {
    this.keys.delete(key);
  }
  hasUnknown() {
    return this.keys.size === 1 && Object.keys(this.to).length === 0;
  }
  writeUnknown() {
    if (this.hasUnknown()) {
      const k2 = this.keys.values().next().value;
      const v2 = this.from[k2];
      this.to.$unknown = [k2, v2];
    }
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/json/jsonReplacer.js
var NUMERIC_CONTROL_CHAR = String.fromCharCode(925);

// node_modules/@aws-sdk/xml-builder/dist-es/escape-attribute.js
var ATTR_ESCAPE_RE = /[&<>"]/g;
var ATTR_ESCAPE_MAP = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;"
};
function escapeAttribute(value) {
  return value.replace(ATTR_ESCAPE_RE, (ch) => ATTR_ESCAPE_MAP[ch]);
}

// node_modules/@aws-sdk/xml-builder/dist-es/escape-element.js
var ELEMENT_ESCAPE_RE = /[&"'<>\r\n\u0085\u2028]/g;
var ELEMENT_ESCAPE_MAP = {
  "&": "&amp;",
  '"': "&quot;",
  "'": "&apos;",
  "<": "&lt;",
  ">": "&gt;",
  "\r": "&#x0D;",
  "\n": "&#x0A;",
  "": "&#x85;",
  "\u2028": "&#x2028;"
};
function escapeElement(value) {
  return value.replace(ELEMENT_ESCAPE_RE, (ch) => ELEMENT_ESCAPE_MAP[ch]);
}

// node_modules/@aws-sdk/xml-builder/dist-es/XmlText.js
var XmlText = class {
  value;
  constructor(value) {
    this.value = value;
  }
  toString() {
    return escapeElement("" + this.value);
  }
};

// node_modules/@aws-sdk/xml-builder/dist-es/XmlNode.js
var XmlNode = class _XmlNode {
  name;
  children;
  attributes = {};
  static of(name, childText, withName) {
    const node = new _XmlNode(name);
    if (childText !== void 0) {
      node.addChildNode(new XmlText(childText));
    }
    if (withName !== void 0) {
      node.withName(withName);
    }
    return node;
  }
  constructor(name, children = []) {
    this.name = name;
    this.children = children;
  }
  withName(name) {
    this.name = name;
    return this;
  }
  addAttribute(name, value) {
    this.attributes[name] = value;
    return this;
  }
  addChildNode(child) {
    this.children.push(child);
    return this;
  }
  removeAttribute(name) {
    delete this.attributes[name];
    return this;
  }
  n(name) {
    this.name = name;
    return this;
  }
  c(child) {
    this.children.push(child);
    return this;
  }
  a(name, value) {
    if (value != null) {
      this.attributes[name] = value;
    }
    return this;
  }
  cc(input, field, withName = field) {
    if (input[field] != null) {
      const node = _XmlNode.of(field, input[field]).withName(withName);
      this.c(node);
    }
  }
  l(input, listName, memberName, valueProvider) {
    if (input[listName] != null) {
      const nodes2 = valueProvider();
      nodes2.map((node) => {
        node.withName(memberName);
        this.c(node);
      });
    }
  }
  lc(input, listName, memberName, valueProvider) {
    if (input[listName] != null) {
      const nodes2 = valueProvider();
      const containerNode = new _XmlNode(memberName);
      nodes2.map((node) => {
        containerNode.c(node);
      });
      this.c(containerNode);
    }
  }
  toString() {
    const hasChildren = Boolean(this.children.length);
    let xmlText = `<${this.name}`;
    const attributes = this.attributes;
    for (const attributeName of Object.keys(attributes)) {
      const attribute = attributes[attributeName];
      if (attribute != null) {
        xmlText += ` ${attributeName}="${escapeAttribute("" + attribute)}"`;
      }
    }
    return xmlText += !hasChildren ? "/>" : `>${this.children.map((c2) => c2.toString()).join("")}</${this.name}>`;
  }
};

// node_modules/@aws-sdk/xml-builder/dist-es/xml-parser.browser.js
var parser;
function parseXML(xmlString) {
  if (!parser) {
    parser = new DOMParser();
  }
  const xmlDocument = parser.parseFromString(xmlString, "application/xml");
  if (xmlDocument.getElementsByTagName("parsererror").length > 0) {
    throw new Error("DOMParser XML parsing error.");
  }
  const xmlToObj = (node) => {
    if (node.nodeType === Node.TEXT_NODE) {
      if (node.textContent?.trim()) {
        return node.textContent;
      }
    }
    if (node.nodeType === Node.ELEMENT_NODE) {
      const element = node;
      if (element.attributes.length === 0 && element.childNodes.length === 0) {
        return "";
      }
      const obj = {};
      const attributes = Array.from(element.attributes);
      for (const attr of attributes) {
        obj[`${attr.name}`] = attr.value;
      }
      const childNodes = Array.from(element.childNodes);
      for (const child of childNodes) {
        const childResult = xmlToObj(child);
        if (childResult != null) {
          const childName = child.nodeName;
          if (childNodes.length === 1 && attributes.length === 0 && childName === "#text") {
            return childResult;
          }
          if (obj[childName]) {
            if (Array.isArray(obj[childName])) {
              obj[childName].push(childResult);
            } else {
              obj[childName] = [obj[childName], childResult];
            }
          } else {
            obj[childName] = childResult;
          }
        } else if (childNodes.length === 1 && attributes.length === 0) {
          return element.textContent;
        }
      }
      return obj;
    }
    return null;
  };
  return {
    [xmlDocument.documentElement.nodeName]: xmlToObj(xmlDocument.documentElement)
  };
}

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/xml/XmlShapeDeserializer.js
var XmlShapeDeserializer = class extends SerdeContextConfig {
  settings;
  stringDeserializer;
  constructor(settings) {
    super();
    this.settings = settings;
    this.stringDeserializer = new FromStringShapeDeserializer(settings);
  }
  setSerdeContext(serdeContext) {
    this.serdeContext = serdeContext;
    this.stringDeserializer.setSerdeContext(serdeContext);
  }
  read(schema, bytes, key) {
    const ns = NormalizedSchema.of(schema);
    const memberSchemas = ns.getMemberSchemas();
    const isEventPayload = ns.isStructSchema() && ns.isMemberSchema() && !!Object.values(memberSchemas).find((memberNs) => {
      return !!memberNs.getMemberTraits().eventPayload;
    });
    if (isEventPayload) {
      const output = {};
      const memberName = Object.keys(memberSchemas)[0];
      const eventMemberSchema = memberSchemas[memberName];
      if (eventMemberSchema.isBlobSchema()) {
        output[memberName] = bytes;
      } else {
        output[memberName] = this.read(memberSchemas[memberName], bytes);
      }
      return output;
    }
    const xmlString = (this.serdeContext?.utf8Encoder ?? toUtf82)(bytes);
    const parsedObject = this.parseXml(xmlString);
    return this.readSchema(schema, key ? parsedObject[key] : parsedObject);
  }
  readSchema(_schema, value) {
    const ns = NormalizedSchema.of(_schema);
    if (ns.isUnitSchema()) {
      return;
    }
    const traits = ns.getMergedTraits();
    if (ns.isListSchema() && !Array.isArray(value)) {
      return this.readSchema(ns, [value]);
    }
    if (value == null) {
      return value;
    }
    if (typeof value === "object") {
      const flat = !!traits.xmlFlattened;
      if (ns.isListSchema()) {
        const listValue = ns.getValueSchema();
        const buffer2 = [];
        const sourceKey = listValue.getMergedTraits().xmlName ?? "member";
        const source = flat ? value : (value[0] ?? value)[sourceKey];
        if (source == null) {
          return buffer2;
        }
        const sourceArray = Array.isArray(source) ? source : [source];
        for (const v2 of sourceArray) {
          buffer2.push(this.readSchema(listValue, v2));
        }
        return buffer2;
      }
      const buffer = {};
      if (ns.isMapSchema()) {
        const keyNs = ns.getKeySchema();
        const memberNs = ns.getValueSchema();
        let entries;
        if (flat) {
          entries = Array.isArray(value) ? value : [value];
        } else {
          entries = Array.isArray(value.entry) ? value.entry : [value.entry];
        }
        const keyProperty = keyNs.getMergedTraits().xmlName ?? "key";
        const valueProperty = memberNs.getMergedTraits().xmlName ?? "value";
        for (const entry of entries) {
          const key = entry[keyProperty];
          const value2 = entry[valueProperty];
          buffer[key] = this.readSchema(memberNs, value2);
        }
        return buffer;
      }
      if (ns.isStructSchema()) {
        const union = ns.isUnionSchema();
        let unionSerde;
        if (union) {
          unionSerde = new UnionSerde(value, buffer);
        }
        for (const [memberName, memberSchema] of ns.structIterator()) {
          const memberTraits = memberSchema.getMergedTraits();
          const xmlObjectKey = !memberTraits.httpPayload ? memberSchema.getMemberTraits().xmlName ?? memberName : memberTraits.xmlName ?? memberSchema.getName();
          if (union) {
            unionSerde.mark(xmlObjectKey);
          }
          if (value[xmlObjectKey] != null) {
            buffer[memberName] = this.readSchema(memberSchema, value[xmlObjectKey]);
          }
        }
        if (union) {
          unionSerde.writeUnknown();
        }
        return buffer;
      }
      if (ns.isDocumentSchema()) {
        return value;
      }
      throw new Error(`@aws-sdk/core/protocols - xml deserializer unhandled schema type for ${ns.getName(true)}`);
    }
    if (ns.isListSchema()) {
      return [];
    }
    if (ns.isMapSchema() || ns.isStructSchema()) {
      return {};
    }
    return this.stringDeserializer.read(ns, value);
  }
  parseXml(xml) {
    if (xml.length) {
      let parsedObj;
      try {
        parsedObj = parseXML(xml);
      } catch (e2) {
        if (e2 && typeof e2 === "object") {
          Object.defineProperty(e2, "$responseBodyText", {
            value: xml
          });
        }
        throw e2;
      }
      const textNodeName = "#text";
      const key = Object.keys(parsedObj)[0];
      const parsedObjToReturn = parsedObj[key];
      if (parsedObjToReturn[textNodeName]) {
        parsedObjToReturn[key] = parsedObjToReturn[textNodeName];
        delete parsedObjToReturn[textNodeName];
      }
      return getValueFromTextNode(parsedObjToReturn);
    }
    return {};
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/xml/parseXmlBody.js
var loadRestXmlErrorCode = (output, data2) => {
  if (data2?.Error?.Code !== void 0) {
    return data2.Error.Code;
  }
  if (data2?.Code !== void 0) {
    return data2.Code;
  }
  if (output.statusCode == 404) {
    return "NotFound";
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/xml/XmlShapeSerializer.js
var XmlShapeSerializer = class extends SerdeContextConfig {
  settings;
  stringBuffer;
  byteBuffer;
  buffer;
  constructor(settings) {
    super();
    this.settings = settings;
  }
  write(schema, value) {
    const ns = NormalizedSchema.of(schema);
    if (ns.isStringSchema() && typeof value === "string") {
      this.stringBuffer = value;
    } else if (ns.isBlobSchema()) {
      this.byteBuffer = "byteLength" in value ? value : (this.serdeContext?.base64Decoder ?? fromBase642)(value);
    } else {
      this.buffer = this.writeStruct(ns, value, void 0);
      const traits = ns.getMergedTraits();
      if (traits.httpPayload && !traits.xmlName) {
        this.buffer.withName(ns.getName());
      }
    }
  }
  flush() {
    if (this.byteBuffer !== void 0) {
      const bytes = this.byteBuffer;
      delete this.byteBuffer;
      return bytes;
    }
    if (this.stringBuffer !== void 0) {
      const str = this.stringBuffer;
      delete this.stringBuffer;
      return str;
    }
    const buffer = this.buffer;
    if (this.settings.xmlNamespace) {
      if (!buffer?.attributes?.["xmlns"]) {
        buffer.addAttribute("xmlns", this.settings.xmlNamespace);
      }
    }
    delete this.buffer;
    return buffer.toString();
  }
  writeStruct(ns, value, parentXmlns) {
    const traits = ns.getMergedTraits();
    const name = ns.isMemberSchema() && !traits.httpPayload ? ns.getMemberTraits().xmlName ?? ns.getMemberName() : traits.xmlName ?? ns.getName();
    if (!name || !ns.isStructSchema()) {
      throw new Error(`@aws-sdk/core/protocols - xml serializer, cannot write struct with empty name or non-struct, schema=${ns.getName(true)}.`);
    }
    const structXmlNode = XmlNode.of(name);
    const [xmlnsAttr, xmlns] = this.getXmlnsAttribute(ns, parentXmlns);
    for (const [memberName, memberSchema] of ns.structIterator()) {
      const val = value[memberName];
      if (val != null || memberSchema.isIdempotencyToken()) {
        if (memberSchema.getMergedTraits().xmlAttribute) {
          structXmlNode.addAttribute(memberSchema.getMergedTraits().xmlName ?? memberName, this.writeSimple(memberSchema, val));
          continue;
        }
        if (memberSchema.isListSchema()) {
          this.writeList(memberSchema, val, structXmlNode, xmlns);
        } else if (memberSchema.isMapSchema()) {
          this.writeMap(memberSchema, val, structXmlNode, xmlns);
        } else if (memberSchema.isStructSchema()) {
          structXmlNode.addChildNode(this.writeStruct(memberSchema, val, xmlns));
        } else {
          const memberNode = XmlNode.of(memberSchema.getMergedTraits().xmlName ?? memberSchema.getMemberName());
          this.writeSimpleInto(memberSchema, val, memberNode, xmlns);
          structXmlNode.addChildNode(memberNode);
        }
      }
    }
    const { $unknown } = value;
    if ($unknown && ns.isUnionSchema() && Array.isArray($unknown) && Object.keys(value).length === 1) {
      const [k2, v2] = $unknown;
      const node = XmlNode.of(k2);
      if (typeof v2 !== "string") {
        if (value instanceof XmlNode || value instanceof XmlText) {
          structXmlNode.addChildNode(value);
        } else {
          throw new Error(`@aws-sdk - $unknown union member in XML requires value of type string, @aws-sdk/xml-builder::XmlNode or XmlText.`);
        }
      }
      this.writeSimpleInto(0, v2, node, xmlns);
      structXmlNode.addChildNode(node);
    }
    if (xmlns) {
      structXmlNode.addAttribute(xmlnsAttr, xmlns);
    }
    return structXmlNode;
  }
  writeList(listMember, array, container, parentXmlns) {
    if (!listMember.isMemberSchema()) {
      throw new Error(`@aws-sdk/core/protocols - xml serializer, cannot write non-member list: ${listMember.getName(true)}`);
    }
    const listTraits = listMember.getMergedTraits();
    const listValueSchema = listMember.getValueSchema();
    const listValueTraits = listValueSchema.getMergedTraits();
    const sparse = !!listValueTraits.sparse;
    const flat = !!listTraits.xmlFlattened;
    const [xmlnsAttr, xmlns] = this.getXmlnsAttribute(listMember, parentXmlns);
    const writeItem = (container2, value) => {
      if (listValueSchema.isListSchema()) {
        this.writeList(listValueSchema, Array.isArray(value) ? value : [value], container2, xmlns);
      } else if (listValueSchema.isMapSchema()) {
        this.writeMap(listValueSchema, value, container2, xmlns);
      } else if (listValueSchema.isStructSchema()) {
        const struct = this.writeStruct(listValueSchema, value, xmlns);
        container2.addChildNode(struct.withName(flat ? listTraits.xmlName ?? listMember.getMemberName() : listValueTraits.xmlName ?? "member"));
      } else {
        const listItemNode = XmlNode.of(flat ? listTraits.xmlName ?? listMember.getMemberName() : listValueTraits.xmlName ?? "member");
        this.writeSimpleInto(listValueSchema, value, listItemNode, xmlns);
        container2.addChildNode(listItemNode);
      }
    };
    if (flat) {
      for (const value of array) {
        if (sparse || value != null) {
          writeItem(container, value);
        }
      }
    } else {
      const listNode = XmlNode.of(listTraits.xmlName ?? listMember.getMemberName());
      if (xmlns) {
        listNode.addAttribute(xmlnsAttr, xmlns);
      }
      for (const value of array) {
        if (sparse || value != null) {
          writeItem(listNode, value);
        }
      }
      container.addChildNode(listNode);
    }
  }
  writeMap(mapMember, map2, container, parentXmlns, containerIsMap = false) {
    if (!mapMember.isMemberSchema()) {
      throw new Error(`@aws-sdk/core/protocols - xml serializer, cannot write non-member map: ${mapMember.getName(true)}`);
    }
    const mapTraits = mapMember.getMergedTraits();
    const mapKeySchema = mapMember.getKeySchema();
    const mapKeyTraits = mapKeySchema.getMergedTraits();
    const keyTag = mapKeyTraits.xmlName ?? "key";
    const mapValueSchema = mapMember.getValueSchema();
    const mapValueTraits = mapValueSchema.getMergedTraits();
    const valueTag = mapValueTraits.xmlName ?? "value";
    const sparse = !!mapValueTraits.sparse;
    const flat = !!mapTraits.xmlFlattened;
    const [xmlnsAttr, xmlns] = this.getXmlnsAttribute(mapMember, parentXmlns);
    const addKeyValue = (entry, key, val) => {
      const keyNode = XmlNode.of(keyTag, key);
      const [keyXmlnsAttr, keyXmlns] = this.getXmlnsAttribute(mapKeySchema, xmlns);
      if (keyXmlns) {
        keyNode.addAttribute(keyXmlnsAttr, keyXmlns);
      }
      entry.addChildNode(keyNode);
      let valueNode = XmlNode.of(valueTag);
      if (mapValueSchema.isListSchema()) {
        this.writeList(mapValueSchema, val, valueNode, xmlns);
      } else if (mapValueSchema.isMapSchema()) {
        this.writeMap(mapValueSchema, val, valueNode, xmlns, true);
      } else if (mapValueSchema.isStructSchema()) {
        valueNode = this.writeStruct(mapValueSchema, val, xmlns);
      } else {
        this.writeSimpleInto(mapValueSchema, val, valueNode, xmlns);
      }
      entry.addChildNode(valueNode);
    };
    if (flat) {
      for (const key in map2) {
        const val = map2[key];
        if (sparse || val != null) {
          const entry = XmlNode.of(mapTraits.xmlName ?? mapMember.getMemberName());
          addKeyValue(entry, key, val);
          container.addChildNode(entry);
        }
      }
    } else {
      let mapNode;
      if (!containerIsMap) {
        mapNode = XmlNode.of(mapTraits.xmlName ?? mapMember.getMemberName());
        if (xmlns) {
          mapNode.addAttribute(xmlnsAttr, xmlns);
        }
        container.addChildNode(mapNode);
      }
      for (const key in map2) {
        const val = map2[key];
        if (sparse || val != null) {
          const entry = XmlNode.of("entry");
          addKeyValue(entry, key, val);
          (containerIsMap ? container : mapNode).addChildNode(entry);
        }
      }
    }
  }
  writeSimple(_schema, value) {
    if (null === value) {
      throw new Error("@aws-sdk/core/protocols - (XML serializer) cannot write null value.");
    }
    const ns = NormalizedSchema.of(_schema);
    let nodeContents = null;
    if (value && typeof value === "object") {
      if (ns.isBlobSchema()) {
        nodeContents = (this.serdeContext?.base64Encoder ?? toBase642)(value);
      } else if (ns.isTimestampSchema() && value instanceof Date) {
        const format = determineTimestampFormat(ns, this.settings);
        switch (format) {
          case 5:
            nodeContents = value.toISOString().replace(".000Z", "Z");
            break;
          case 6:
            nodeContents = dateToUtcString(value);
            break;
          case 7:
            nodeContents = String(value.getTime() / 1e3);
            break;
          default:
            console.warn("Missing timestamp format, using http date", value);
            nodeContents = dateToUtcString(value);
            break;
        }
      } else if (ns.isBigDecimalSchema() && value) {
        if (value instanceof NumericValue) {
          return value.string;
        }
        return String(value);
      } else if (ns.isMapSchema() || ns.isListSchema()) {
        throw new Error("@aws-sdk/core/protocols - xml serializer, cannot call _write() on List/Map schema, call writeList or writeMap() instead.");
      } else {
        throw new Error(`@aws-sdk/core/protocols - xml serializer, unhandled schema type for object value and schema: ${ns.getName(true)}`);
      }
    }
    if (ns.isBooleanSchema() || ns.isNumericSchema() || ns.isBigIntegerSchema() || ns.isBigDecimalSchema()) {
      nodeContents = String(value);
    }
    if (ns.isStringSchema()) {
      if (value === void 0 && ns.isIdempotencyToken()) {
        nodeContents = generateIdempotencyToken();
      } else {
        nodeContents = String(value);
      }
    }
    if (nodeContents === null) {
      throw new Error(`Unhandled schema-value pair ${ns.getName(true)}=${value}`);
    }
    return nodeContents;
  }
  writeSimpleInto(_schema, value, into, parentXmlns) {
    const nodeContents = this.writeSimple(_schema, value);
    const ns = NormalizedSchema.of(_schema);
    const content = new XmlText(nodeContents);
    const [xmlnsAttr, xmlns] = this.getXmlnsAttribute(ns, parentXmlns);
    if (xmlns) {
      into.addAttribute(xmlnsAttr, xmlns);
    }
    into.addChildNode(content);
  }
  getXmlnsAttribute(ns, parentXmlns) {
    const traits = ns.getMergedTraits();
    const [prefix, xmlns] = traits.xmlNamespace ?? [];
    if (xmlns && xmlns !== parentXmlns) {
      return [prefix ? `xmlns:${prefix}` : "xmlns", xmlns];
    }
    return [void 0, void 0];
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/xml/XmlCodec.js
var XmlCodec = class extends SerdeContextConfig {
  settings;
  constructor(settings) {
    super();
    this.settings = settings;
  }
  createSerializer() {
    const serializer = new XmlShapeSerializer(this.settings);
    serializer.setSerdeContext(this.serdeContext);
    return serializer;
  }
  createDeserializer() {
    const deserializer = new XmlShapeDeserializer(this.settings);
    deserializer.setSerdeContext(this.serdeContext);
    return deserializer;
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/protocols/xml/AwsRestXmlProtocol.js
var AwsRestXmlProtocol = class _AwsRestXmlProtocol extends HttpBindingProtocol {
  codec;
  serializer;
  deserializer;
  mixin = new ProtocolLib();
  constructor(options) {
    super(options);
    const settings = {
      timestampFormat: {
        useTrait: true,
        default: 5
      },
      httpBindings: true,
      xmlNamespace: options.xmlNamespace,
      serviceNamespace: options.defaultNamespace
    };
    this.codec = new XmlCodec(settings);
    this.serializer = new HttpInterceptingShapeSerializer(this.codec.createSerializer(), settings);
    this.deserializer = new HttpInterceptingShapeDeserializer(this.codec.createDeserializer(), settings);
  }
  getPayloadCodec() {
    return this.codec;
  }
  getShapeId() {
    return "aws.protocols#restXml";
  }
  serializeRequest(operationSchema, input, context) {
    return __async(this, null, function* () {
      const request = yield __superGet(_AwsRestXmlProtocol.prototype, this, "serializeRequest").call(this, operationSchema, input, context);
      const inputSchema = NormalizedSchema.of(operationSchema.input);
      if (!request.headers["content-type"]) {
        const contentType = this.mixin.resolveRestContentType(this.getDefaultContentType(), inputSchema);
        if (contentType) {
          request.headers["content-type"] = contentType;
        }
      }
      if (typeof request.body === "string" && request.headers["content-type"] === this.getDefaultContentType() && !request.body.startsWith("<?xml ") && !this.hasUnstructuredPayloadBinding(inputSchema)) {
        request.body = '<?xml version="1.0" encoding="UTF-8"?>' + request.body;
      }
      return request;
    });
  }
  deserializeResponse(operationSchema, context, response) {
    return __async(this, null, function* () {
      return __superGet(_AwsRestXmlProtocol.prototype, this, "deserializeResponse").call(this, operationSchema, context, response);
    });
  }
  handleError(operationSchema, context, response, dataObject, metadata) {
    return __async(this, null, function* () {
      const errorIdentifier = loadRestXmlErrorCode(response, dataObject) ?? "Unknown";
      this.mixin.compose(this.compositeErrorRegistry, errorIdentifier, this.options.defaultNamespace);
      if (dataObject.Error && typeof dataObject.Error === "object") {
        for (const key of Object.keys(dataObject.Error)) {
          dataObject[key] = dataObject.Error[key];
          if (key.toLowerCase() === "message") {
            dataObject.message = dataObject.Error[key];
          }
        }
      }
      if (dataObject.RequestId && !metadata.requestId) {
        metadata.requestId = dataObject.RequestId;
      }
      const { errorSchema, errorMetadata } = yield this.mixin.getErrorSchemaOrThrowBaseException(errorIdentifier, this.options.defaultNamespace, response, dataObject, metadata);
      const ns = NormalizedSchema.of(errorSchema);
      const message = dataObject.Error?.message ?? dataObject.Error?.Message ?? dataObject.message ?? dataObject.Message ?? "UnknownError";
      const ErrorCtor = this.compositeErrorRegistry.getErrorCtor(errorSchema) ?? Error;
      const exception = new ErrorCtor(message);
      yield this.deserializeHttpMessage(errorSchema, context, response, dataObject);
      const output = {};
      const errorDeserializer = this.codec.createDeserializer();
      for (const [name, member] of ns.structIterator()) {
        const target = member.getMergedTraits().xmlName ?? name;
        const value = dataObject.Error?.[target] ?? dataObject[target];
        output[name] = errorDeserializer.readSchema(member, value);
      }
      throw this.mixin.decorateServiceException(Object.assign(exception, errorMetadata, {
        $fault: ns.getMergedTraits().error,
        message
      }, output), dataObject);
    });
  }
  getDefaultContentType() {
    return "application/xml";
  }
  hasUnstructuredPayloadBinding(ns) {
    for (const [, member] of ns.structIterator()) {
      if (member.getMergedTraits().httpPayload) {
        return !(member.isStructSchema() || member.isMapSchema() || member.isListSchema());
      }
    }
    return false;
  }
};

// node_modules/@aws-sdk/middleware-sdk-s3/dist-es/protocol/S3RestXmlProtocol.js
var S3RestXmlProtocol = class _S3RestXmlProtocol extends AwsRestXmlProtocol {
  serializeRequest(operationSchema, input, context) {
    return __async(this, null, function* () {
      const request = yield __superGet(_S3RestXmlProtocol.prototype, this, "serializeRequest").call(this, operationSchema, input, context);
      const ns = NormalizedSchema.of(operationSchema.input);
      const staticStructureSchema = ns.getSchema();
      let bucketMemberIndex = 0;
      const requiredMemberCount = staticStructureSchema[6] ?? 0;
      if (input && typeof input === "object") {
        for (const [memberName, memberNs] of ns.structIterator()) {
          if (++bucketMemberIndex > requiredMemberCount) {
            break;
          }
          if (memberName === "Bucket") {
            if (!input.Bucket && memberNs.getMergedTraits().httpLabel) {
              throw new Error(`No value provided for input HTTP label: Bucket.`);
            }
            break;
          }
        }
      }
      return request;
    });
  }
};

// node_modules/@aws-sdk/middleware-user-agent/dist-es/configurations.js
var DEFAULT_UA_APP_ID = void 0;
function isValidUserAgentAppId(appId) {
  if (appId === void 0) {
    return true;
  }
  return typeof appId === "string" && appId.length <= 50;
}
function resolveUserAgentConfig(input) {
  const normalizedAppIdProvider = normalizeProvider2(input.userAgentAppId ?? DEFAULT_UA_APP_ID);
  const { customUserAgent } = input;
  return Object.assign(input, {
    customUserAgent: typeof customUserAgent === "string" ? [[customUserAgent]] : customUserAgent,
    userAgentAppId: () => __async(this, null, function* () {
      const appId = yield normalizedAppIdProvider();
      if (!isValidUserAgentAppId(appId)) {
        const logger2 = input.logger?.constructor?.name === "NoOpLogger" || !input.logger ? console : input.logger;
        if (typeof appId !== "string") {
          logger2?.warn("userAgentAppId must be a string or undefined.");
        } else if (appId.length > 50) {
          logger2?.warn("The provided userAgentAppId exceeds the maximum length of 50 characters.");
        }
      }
      return appId;
    })
  });
}

// node_modules/@aws-sdk/middleware-user-agent/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/isVirtualHostableS3Bucket.js
var isVirtualHostableS3Bucket = (value, allowSubDomains = false) => {
  if (allowSubDomains) {
    for (const label of value.split(".")) {
      if (!isVirtualHostableS3Bucket(label)) {
        return false;
      }
    }
    return true;
  }
  if (!isValidHostLabel(value)) {
    return false;
  }
  if (value.length < 3 || value.length > 63) {
    return false;
  }
  if (value !== value.toLowerCase()) {
    return false;
  }
  if (isIpAddress(value)) {
    return false;
  }
  return true;
};

// node_modules/@aws-sdk/middleware-user-agent/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/parseArn.js
var ARN_DELIMITER = ":";
var RESOURCE_DELIMITER = "/";
var parseArn = (value) => {
  const segments = value.split(ARN_DELIMITER);
  if (segments.length < 6)
    return null;
  const [arn, partition3, service, region, accountId, ...resourcePath] = segments;
  if (arn !== "arn" || partition3 === "" || service === "" || resourcePath.join(ARN_DELIMITER) === "")
    return null;
  const resourceId = resourcePath.map((resource) => resource.split(RESOURCE_DELIMITER)).flat();
  return {
    partition: partition3,
    service,
    region,
    accountId,
    resourceId
  };
};

// node_modules/@aws-sdk/middleware-user-agent/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/partitions.json
var partitions_default = {
  partitions: [{
    id: "aws",
    outputs: {
      dnsSuffix: "amazonaws.com",
      dualStackDnsSuffix: "api.aws",
      implicitGlobalRegion: "us-east-1",
      name: "aws",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^(us|eu|ap|sa|ca|me|af|il|mx)\\-\\w+\\-\\d+$",
    regions: {
      "af-south-1": {
        description: "Africa (Cape Town)"
      },
      "ap-east-1": {
        description: "Asia Pacific (Hong Kong)"
      },
      "ap-east-2": {
        description: "Asia Pacific (Taipei)"
      },
      "ap-northeast-1": {
        description: "Asia Pacific (Tokyo)"
      },
      "ap-northeast-2": {
        description: "Asia Pacific (Seoul)"
      },
      "ap-northeast-3": {
        description: "Asia Pacific (Osaka)"
      },
      "ap-south-1": {
        description: "Asia Pacific (Mumbai)"
      },
      "ap-south-2": {
        description: "Asia Pacific (Hyderabad)"
      },
      "ap-southeast-1": {
        description: "Asia Pacific (Singapore)"
      },
      "ap-southeast-2": {
        description: "Asia Pacific (Sydney)"
      },
      "ap-southeast-3": {
        description: "Asia Pacific (Jakarta)"
      },
      "ap-southeast-4": {
        description: "Asia Pacific (Melbourne)"
      },
      "ap-southeast-5": {
        description: "Asia Pacific (Malaysia)"
      },
      "ap-southeast-6": {
        description: "Asia Pacific (New Zealand)"
      },
      "ap-southeast-7": {
        description: "Asia Pacific (Thailand)"
      },
      "aws-global": {
        description: "aws global region"
      },
      "ca-central-1": {
        description: "Canada (Central)"
      },
      "ca-west-1": {
        description: "Canada West (Calgary)"
      },
      "eu-central-1": {
        description: "Europe (Frankfurt)"
      },
      "eu-central-2": {
        description: "Europe (Zurich)"
      },
      "eu-north-1": {
        description: "Europe (Stockholm)"
      },
      "eu-south-1": {
        description: "Europe (Milan)"
      },
      "eu-south-2": {
        description: "Europe (Spain)"
      },
      "eu-west-1": {
        description: "Europe (Ireland)"
      },
      "eu-west-2": {
        description: "Europe (London)"
      },
      "eu-west-3": {
        description: "Europe (Paris)"
      },
      "il-central-1": {
        description: "Israel (Tel Aviv)"
      },
      "me-central-1": {
        description: "Middle East (UAE)"
      },
      "me-south-1": {
        description: "Middle East (Bahrain)"
      },
      "mx-central-1": {
        description: "Mexico (Central)"
      },
      "sa-east-1": {
        description: "South America (Sao Paulo)"
      },
      "us-east-1": {
        description: "US East (N. Virginia)"
      },
      "us-east-2": {
        description: "US East (Ohio)"
      },
      "us-west-1": {
        description: "US West (N. California)"
      },
      "us-west-2": {
        description: "US West (Oregon)"
      }
    }
  }, {
    id: "aws-cn",
    outputs: {
      dnsSuffix: "amazonaws.com.cn",
      dualStackDnsSuffix: "api.amazonwebservices.com.cn",
      implicitGlobalRegion: "cn-northwest-1",
      name: "aws-cn",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^cn\\-\\w+\\-\\d+$",
    regions: {
      "aws-cn-global": {
        description: "aws-cn global region"
      },
      "cn-north-1": {
        description: "China (Beijing)"
      },
      "cn-northwest-1": {
        description: "China (Ningxia)"
      }
    }
  }, {
    id: "aws-eusc",
    outputs: {
      dnsSuffix: "amazonaws.eu",
      dualStackDnsSuffix: "api.amazonwebservices.eu",
      implicitGlobalRegion: "eusc-de-east-1",
      name: "aws-eusc",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^eusc\\-(de)\\-\\w+\\-\\d+$",
    regions: {
      "eusc-de-east-1": {
        description: "AWS European Sovereign Cloud (Germany)"
      }
    }
  }, {
    id: "aws-iso",
    outputs: {
      dnsSuffix: "c2s.ic.gov",
      dualStackDnsSuffix: "api.aws.ic.gov",
      implicitGlobalRegion: "us-iso-east-1",
      name: "aws-iso",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-iso\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-global": {
        description: "aws-iso global region"
      },
      "us-iso-east-1": {
        description: "US ISO East"
      },
      "us-iso-west-1": {
        description: "US ISO WEST"
      }
    }
  }, {
    id: "aws-iso-b",
    outputs: {
      dnsSuffix: "sc2s.sgov.gov",
      dualStackDnsSuffix: "api.aws.scloud",
      implicitGlobalRegion: "us-isob-east-1",
      name: "aws-iso-b",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-isob\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-b-global": {
        description: "aws-iso-b global region"
      },
      "us-isob-east-1": {
        description: "US ISOB East (Ohio)"
      },
      "us-isob-west-1": {
        description: "US ISOB West"
      }
    }
  }, {
    id: "aws-iso-e",
    outputs: {
      dnsSuffix: "cloud.adc-e.uk",
      dualStackDnsSuffix: "api.cloud-aws.adc-e.uk",
      implicitGlobalRegion: "eu-isoe-west-1",
      name: "aws-iso-e",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^eu\\-isoe\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-e-global": {
        description: "aws-iso-e global region"
      },
      "eu-isoe-west-1": {
        description: "EU ISOE West"
      }
    }
  }, {
    id: "aws-iso-f",
    outputs: {
      dnsSuffix: "csp.hci.ic.gov",
      dualStackDnsSuffix: "api.aws.hci.ic.gov",
      implicitGlobalRegion: "us-isof-south-1",
      name: "aws-iso-f",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-isof\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-f-global": {
        description: "aws-iso-f global region"
      },
      "us-isof-east-1": {
        description: "US ISOF EAST"
      },
      "us-isof-south-1": {
        description: "US ISOF SOUTH"
      }
    }
  }, {
    id: "aws-us-gov",
    outputs: {
      dnsSuffix: "amazonaws.com",
      dualStackDnsSuffix: "api.aws",
      implicitGlobalRegion: "us-gov-west-1",
      name: "aws-us-gov",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-gov\\-\\w+\\-\\d+$",
    regions: {
      "aws-us-gov-global": {
        description: "aws-us-gov global region"
      },
      "us-gov-east-1": {
        description: "AWS GovCloud (US-East)"
      },
      "us-gov-west-1": {
        description: "AWS GovCloud (US-West)"
      }
    }
  }],
  version: "1.1"
};

// node_modules/@aws-sdk/middleware-user-agent/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/partition.js
var selectedPartitionsInfo = partitions_default;
var selectedUserAgentPrefix = "";
var partition = (value) => {
  const { partitions } = selectedPartitionsInfo;
  for (const partition3 of partitions) {
    const { regions, outputs } = partition3;
    for (const [region, regionData] of Object.entries(regions)) {
      if (region === value) {
        return __spreadValues(__spreadValues({}, outputs), regionData);
      }
    }
  }
  for (const partition3 of partitions) {
    const { regionRegex, outputs } = partition3;
    if (new RegExp(regionRegex).test(value)) {
      return __spreadValues({}, outputs);
    }
  }
  const DEFAULT_PARTITION = partitions.find((partition3) => partition3.id === "aws");
  if (!DEFAULT_PARTITION) {
    throw new Error("Provided region was not found in the partition array or regex, and default partition with id 'aws' doesn't exist.");
  }
  return __spreadValues({}, DEFAULT_PARTITION.outputs);
};
var getUserAgentPrefix = () => selectedUserAgentPrefix;

// node_modules/@aws-sdk/middleware-user-agent/node_modules/@aws-sdk/util-endpoints/dist-es/aws.js
var awsEndpointFunctions = {
  isVirtualHostableS3Bucket,
  parseArn,
  partition
};
customEndpointFunctions.aws = awsEndpointFunctions;

// node_modules/@aws-sdk/middleware-user-agent/dist-es/check-features.js
var ACCOUNT_ID_ENDPOINT_REGEX = /\d{12}\.ddb/;
function checkFeatures(context, config, args) {
  return __async(this, null, function* () {
    const request = args.request;
    if (request?.headers?.["smithy-protocol"] === "rpc-v2-cbor") {
      setFeature(context, "PROTOCOL_RPC_V2_CBOR", "M");
    }
    if (typeof config.retryStrategy === "function") {
      const retryStrategy = yield config.retryStrategy();
      if (typeof retryStrategy.mode === "string") {
        switch (retryStrategy.mode) {
          case RETRY_MODES.ADAPTIVE:
            setFeature(context, "RETRY_MODE_ADAPTIVE", "F");
            break;
          case RETRY_MODES.STANDARD:
            setFeature(context, "RETRY_MODE_STANDARD", "E");
            break;
        }
      }
    }
    if (typeof config.accountIdEndpointMode === "function") {
      const endpointV2 = context.endpointV2;
      if (String(endpointV2?.url?.hostname).match(ACCOUNT_ID_ENDPOINT_REGEX)) {
        setFeature(context, "ACCOUNT_ID_ENDPOINT", "O");
      }
      switch (yield config.accountIdEndpointMode?.()) {
        case "disabled":
          setFeature(context, "ACCOUNT_ID_MODE_DISABLED", "Q");
          break;
        case "preferred":
          setFeature(context, "ACCOUNT_ID_MODE_PREFERRED", "P");
          break;
        case "required":
          setFeature(context, "ACCOUNT_ID_MODE_REQUIRED", "R");
          break;
      }
    }
    const identity = context.__smithy_context?.selectedHttpAuthScheme?.identity;
    if (identity?.$source) {
      const credentials = identity;
      if (credentials.accountId) {
        setFeature(context, "RESOLVED_ACCOUNT_ID", "T");
      }
      for (const [key, value] of Object.entries(credentials.$source ?? {})) {
        setFeature(context, key, value);
      }
    }
  });
}

// node_modules/@aws-sdk/middleware-user-agent/dist-es/constants.js
var USER_AGENT = "user-agent";
var X_AMZ_USER_AGENT = "x-amz-user-agent";
var SPACE = " ";
var UA_NAME_SEPARATOR = "/";
var UA_NAME_ESCAPE_REGEX = /[^!$%&'*+\-.^_`|~\w]/g;
var UA_VALUE_ESCAPE_REGEX = /[^!$%&'*+\-.^_`|~\w#]/g;
var UA_ESCAPE_CHAR = "-";

// node_modules/@aws-sdk/middleware-user-agent/dist-es/encode-features.js
var BYTE_LIMIT = 1024;
function encodeFeatures(features) {
  let buffer = "";
  for (const key in features) {
    const val = features[key];
    if (buffer.length + val.length + 1 <= BYTE_LIMIT) {
      if (buffer.length) {
        buffer += "," + val;
      } else {
        buffer += val;
      }
      continue;
    }
    break;
  }
  return buffer;
}

// node_modules/@aws-sdk/middleware-user-agent/dist-es/user-agent-middleware.js
var userAgentMiddleware = (options) => (next, context) => (args) => __async(void 0, null, function* () {
  const { request } = args;
  if (!HttpRequest.isInstance(request)) {
    return next(args);
  }
  const { headers } = request;
  const userAgent = context?.userAgent?.map(escapeUserAgent) || [];
  const defaultUserAgent = (yield options.defaultUserAgentProvider()).map(escapeUserAgent);
  yield checkFeatures(context, options, args);
  const awsContext = context;
  defaultUserAgent.push(`m/${encodeFeatures(Object.assign({}, context.__smithy_context?.features, awsContext.__aws_sdk_context?.features))}`);
  const customUserAgent = options?.customUserAgent?.map(escapeUserAgent) || [];
  const appId = yield options.userAgentAppId();
  if (appId) {
    defaultUserAgent.push(escapeUserAgent([`app`, `${appId}`]));
  }
  const prefix = getUserAgentPrefix();
  const sdkUserAgentValue = (prefix ? [prefix] : []).concat([...defaultUserAgent, ...userAgent, ...customUserAgent]).join(SPACE);
  const normalUAValue = [
    ...defaultUserAgent.filter((section) => section.startsWith("aws-sdk-")),
    ...customUserAgent
  ].join(SPACE);
  if (options.runtime !== "browser") {
    if (normalUAValue) {
      headers[X_AMZ_USER_AGENT] = headers[X_AMZ_USER_AGENT] ? `${headers[USER_AGENT]} ${normalUAValue}` : normalUAValue;
    }
    headers[USER_AGENT] = sdkUserAgentValue;
  } else {
    headers[X_AMZ_USER_AGENT] = sdkUserAgentValue;
  }
  return next(__spreadProps(__spreadValues({}, args), {
    request
  }));
});
var escapeUserAgent = (userAgentPair) => {
  const name = userAgentPair[0].split(UA_NAME_SEPARATOR).map((part) => part.replace(UA_NAME_ESCAPE_REGEX, UA_ESCAPE_CHAR)).join(UA_NAME_SEPARATOR);
  const version = userAgentPair[1]?.replace(UA_VALUE_ESCAPE_REGEX, UA_ESCAPE_CHAR);
  const prefixSeparatorIndex = name.indexOf(UA_NAME_SEPARATOR);
  const prefix = name.substring(0, prefixSeparatorIndex);
  let uaName = name.substring(prefixSeparatorIndex + 1);
  if (prefix === "api") {
    uaName = uaName.toLowerCase();
  }
  return [prefix, uaName, version].filter((item) => item && item.length > 0).reduce((acc, item, index) => {
    switch (index) {
      case 0:
        return item;
      case 1:
        return `${acc}/${item}`;
      default:
        return `${acc}#${item}`;
    }
  }, "");
};
var getUserAgentMiddlewareOptions = {
  name: "getUserAgentMiddleware",
  step: "build",
  priority: "low",
  tags: ["SET_USER_AGENT", "USER_AGENT"],
  override: true
};
var getUserAgentPlugin = (config) => ({
  applyToStack: (clientStack) => {
    clientStack.add(userAgentMiddleware(config), getUserAgentMiddlewareOptions);
  }
});

// node_modules/@smithy/core/dist-es/submodules/config/property-provider/memoize.js
var memoize = (provider, isExpired, requiresRefresh) => {
  let resolved;
  let pending;
  let hasResult;
  let isConstant = false;
  const coalesceProvider = () => __async(void 0, null, function* () {
    if (!pending) {
      pending = provider();
    }
    try {
      resolved = yield pending;
      hasResult = true;
      isConstant = false;
    } finally {
      pending = void 0;
    }
    return resolved;
  });
  if (isExpired === void 0) {
    return (options) => __async(void 0, null, function* () {
      if (!hasResult || options?.forceRefresh) {
        resolved = yield coalesceProvider();
      }
      return resolved;
    });
  }
  return (options) => __async(void 0, null, function* () {
    if (!hasResult || options?.forceRefresh) {
      resolved = yield coalesceProvider();
    }
    if (isConstant) {
      return resolved;
    }
    if (requiresRefresh && !requiresRefresh(resolved)) {
      isConstant = true;
      return resolved;
    }
    if (isExpired(resolved)) {
      yield coalesceProvider();
      return resolved;
    }
    return resolved;
  });
};

// node_modules/@smithy/core/dist-es/submodules/config/util-config-provider/types.js
var SelectorType3;
(function(SelectorType4) {
  SelectorType4["ENV"] = "env";
  SelectorType4["CONFIG"] = "shared config entry";
})(SelectorType3 || (SelectorType3 = {}));

// node_modules/@smithy/core/dist-es/submodules/config/config-resolver/regionConfig/checkRegion.js
var validRegions = /* @__PURE__ */ new Set();
var checkRegion = (region, check = isValidHostLabel) => {
  if (!validRegions.has(region) && !check(region)) {
    if (region === "*") {
      console.warn(`@smithy/config-resolver WARN - Please use the caller region instead of "*". See "sigv4a" in https://github.com/aws/aws-sdk-js-v3/blob/main/supplemental-docs/CLIENTS.md.`);
    } else {
      throw new Error(`Region not accepted: region="${region}" is not a valid hostname component.`);
    }
  } else {
    validRegions.add(region);
  }
};

// node_modules/@smithy/core/dist-es/submodules/config/config-resolver/regionConfig/isFipsRegion.js
var isFipsRegion = (region) => typeof region === "string" && (region.startsWith("fips-") || region.endsWith("-fips"));

// node_modules/@smithy/core/dist-es/submodules/config/config-resolver/regionConfig/getRealRegion.js
var getRealRegion = (region) => isFipsRegion(region) ? ["fips-aws-global", "aws-fips"].includes(region) ? "us-east-1" : region.replace(/fips-(dkr-|prod-)?|-fips/, "") : region;

// node_modules/@smithy/core/dist-es/submodules/config/config-resolver/regionConfig/resolveRegionConfig.js
var resolveRegionConfig = (input) => {
  const { region, useFipsEndpoint } = input;
  if (!region) {
    throw new Error("Region is missing");
  }
  return Object.assign(input, {
    region: () => __async(void 0, null, function* () {
      const providedRegion = typeof region === "function" ? yield region() : region;
      const realRegion = getRealRegion(providedRegion);
      checkRegion(realRegion);
      return realRegion;
    }),
    useFipsEndpoint: () => __async(void 0, null, function* () {
      const providedRegion = typeof region === "string" ? region : yield region();
      if (isFipsRegion(providedRegion)) {
        return true;
      }
      return typeof useFipsEndpoint !== "function" ? Promise.resolve(!!useFipsEndpoint) : useFipsEndpoint();
    })
  });
};

// node_modules/@smithy/core/dist-es/submodules/config/defaults-mode/constants.js
var DEFAULTS_MODE_OPTIONS = ["in-region", "cross-region", "mobile", "standard", "legacy"];

// node_modules/@smithy/core/dist-es/submodules/config/defaults-mode/resolveDefaultsModeConfig.browser.js
var resolveDefaultsModeConfig = ({ defaultsMode } = {}) => memoize(() => __async(void 0, null, function* () {
  const mode = typeof defaultsMode === "function" ? yield defaultsMode() : defaultsMode;
  switch (mode?.toLowerCase()) {
    case "auto":
      return Promise.resolve(useMobileConfiguration() ? "mobile" : "standard");
    case "mobile":
    case "in-region":
    case "cross-region":
    case "standard":
    case "legacy":
      return Promise.resolve(mode?.toLocaleLowerCase());
    case void 0:
      return Promise.resolve("legacy");
    default:
      throw new Error(`Invalid parameter for "defaultsMode", expect ${DEFAULTS_MODE_OPTIONS.join(", ")}, got ${mode}`);
  }
}));
var useMobileConfiguration = () => {
  const navigator = window?.navigator;
  if (navigator?.connection) {
    const { effectiveType, rtt, downlink } = navigator?.connection;
    const slow = typeof effectiveType === "string" && effectiveType !== "4g" || Number(rtt) > 100 || Number(downlink) < 10;
    if (slow) {
      return true;
    }
  }
  return navigator?.userAgentData?.mobile || typeof navigator?.maxTouchPoints === "number" && navigator?.maxTouchPoints > 1;
};

// node_modules/@smithy/core/dist-es/submodules/config/index.browser.js
var no2 = Symbol.for("node-only");
var DEFAULT_USE_DUALSTACK_ENDPOINT = false;
var DEFAULT_USE_FIPS_ENDPOINT = false;

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/utils/getDateHeader.js
var getDateHeader = (response) => HttpResponse.isInstance(response) ? response.headers?.date ?? response.headers?.Date : void 0;

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/utils/getSkewCorrectedDate.js
var getSkewCorrectedDate = (systemClockOffset) => new Date(Date.now() + systemClockOffset);

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/utils/isClockSkewed.js
var isClockSkewed = (clockTime, systemClockOffset) => Math.abs(getSkewCorrectedDate(systemClockOffset).getTime() - clockTime) >= 3e5;

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/utils/getUpdatedSystemClockOffset.js
var getUpdatedSystemClockOffset = (clockTime, currentSystemClockOffset) => {
  const clockTimeInMs = Date.parse(clockTime);
  if (isClockSkewed(clockTimeInMs, currentSystemClockOffset)) {
    return clockTimeInMs - Date.now();
  }
  return currentSystemClockOffset;
};

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/AwsSdkSigV4Signer.js
var throwSigningPropertyError = (name, property) => {
  if (!property) {
    throw new Error(`Property \`${name}\` is not resolved for AWS SDK SigV4Auth`);
  }
  return property;
};
var validateSigningProperties = (signingProperties) => __async(void 0, null, function* () {
  const context = throwSigningPropertyError("context", signingProperties.context);
  const config = throwSigningPropertyError("config", signingProperties.config);
  const authScheme = context.endpointV2?.properties?.authSchemes?.[0];
  const signerFunction = throwSigningPropertyError("signer", config.signer);
  const signer = yield signerFunction(authScheme);
  const signingRegion = signingProperties?.signingRegion;
  const signingRegionSet = signingProperties?.signingRegionSet;
  const signingName = signingProperties?.signingName;
  return {
    config,
    signer,
    signingRegion,
    signingRegionSet,
    signingName
  };
});
var AwsSdkSigV4Signer = class {
  sign(httpRequest, identity, signingProperties) {
    return __async(this, null, function* () {
      if (!HttpRequest.isInstance(httpRequest)) {
        throw new Error("The request is not an instance of `HttpRequest` and cannot be signed");
      }
      const validatedProps = yield validateSigningProperties(signingProperties);
      const { config, signer } = validatedProps;
      let { signingRegion, signingName } = validatedProps;
      const handlerExecutionContext = signingProperties.context;
      if (handlerExecutionContext?.authSchemes?.length ?? 0 > 1) {
        const [first, second] = handlerExecutionContext.authSchemes;
        if (first?.name === "sigv4a" && second?.name === "sigv4") {
          signingRegion = second?.signingRegion ?? signingRegion;
          signingName = second?.signingName ?? signingName;
        }
      }
      const signedRequest = yield signer.sign(httpRequest, {
        signingDate: getSkewCorrectedDate(config.systemClockOffset),
        signingRegion,
        signingService: signingName
      });
      return signedRequest;
    });
  }
  errorHandler(signingProperties) {
    return (error) => {
      const serverTime = error.ServerTime ?? getDateHeader(error.$response);
      if (serverTime) {
        const config = throwSigningPropertyError("config", signingProperties.config);
        const initialSystemClockOffset = config.systemClockOffset;
        config.systemClockOffset = getUpdatedSystemClockOffset(serverTime, config.systemClockOffset);
        const clockSkewCorrected = config.systemClockOffset !== initialSystemClockOffset;
        if (clockSkewCorrected && error.$metadata) {
          error.$metadata.clockSkewCorrected = true;
        }
      }
      throw error;
    };
  }
  successHandler(httpResponse, signingProperties) {
    const dateHeader = getDateHeader(httpResponse);
    if (dateHeader) {
      const config = throwSigningPropertyError("config", signingProperties.config);
      config.systemClockOffset = getUpdatedSystemClockOffset(dateHeader, config.systemClockOffset);
    }
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/AwsSdkSigV4ASigner.js
var AwsSdkSigV4ASigner = class extends AwsSdkSigV4Signer {
  sign(httpRequest, identity, signingProperties) {
    return __async(this, null, function* () {
      if (!HttpRequest.isInstance(httpRequest)) {
        throw new Error("The request is not an instance of `HttpRequest` and cannot be signed");
      }
      const { config, signer, signingRegion, signingRegionSet, signingName } = yield validateSigningProperties(signingProperties);
      const configResolvedSigningRegionSet = yield config.sigv4aSigningRegionSet?.();
      const multiRegionOverride = (configResolvedSigningRegionSet ?? signingRegionSet ?? [signingRegion]).join(",");
      const signedRequest = yield signer.sign(httpRequest, {
        signingDate: getSkewCorrectedDate(config.systemClockOffset),
        signingRegion: multiRegionOverride,
        signingService: signingName
      });
      return signedRequest;
    });
  }
};

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/resolveAwsSdkSigV4AConfig.js
var resolveAwsSdkSigV4AConfig = (config) => {
  config.sigv4aSigningRegionSet = normalizeProvider2(config.sigv4aSigningRegionSet);
  return config;
};

// node_modules/@aws-sdk/core/dist-es/submodules/httpAuthSchemes/aws_sdk/resolveAwsSdkSigV4Config.js
var resolveAwsSdkSigV4Config = (config) => {
  let inputCredentials = config.credentials;
  let isUserSupplied = !!config.credentials;
  let resolvedCredentials = void 0;
  Object.defineProperty(config, "credentials", {
    set(credentials) {
      if (credentials && credentials !== inputCredentials && credentials !== resolvedCredentials) {
        isUserSupplied = true;
      }
      inputCredentials = credentials;
      const memoizedProvider = normalizeCredentialProvider(config, {
        credentials: inputCredentials,
        credentialDefaultProvider: config.credentialDefaultProvider
      });
      const boundProvider = bindCallerConfig(config, memoizedProvider);
      if (isUserSupplied && !boundProvider.attributed) {
        const isCredentialObject = typeof inputCredentials === "object" && inputCredentials !== null;
        resolvedCredentials = (options) => __async(this, null, function* () {
          const creds = yield boundProvider(options);
          const attributedCreds = creds;
          if (isCredentialObject && (!attributedCreds.$source || Object.keys(attributedCreds.$source).length === 0)) {
            return setCredentialFeature(attributedCreds, "CREDENTIALS_CODE", "e");
          }
          return attributedCreds;
        });
        resolvedCredentials.memoized = boundProvider.memoized;
        resolvedCredentials.configBound = boundProvider.configBound;
        resolvedCredentials.attributed = true;
      } else {
        resolvedCredentials = boundProvider;
      }
    },
    get() {
      return resolvedCredentials;
    },
    enumerable: true,
    configurable: true
  });
  config.credentials = inputCredentials;
  const { signingEscapePath = true, systemClockOffset = config.systemClockOffset || 0, sha256 } = config;
  let signer;
  if (config.signer) {
    signer = normalizeProvider2(config.signer);
  } else if (config.regionInfoProvider) {
    signer = () => normalizeProvider2(config.region)().then((region) => __async(void 0, null, function* () {
      return [
        (yield config.regionInfoProvider(region, {
          useFipsEndpoint: yield config.useFipsEndpoint(),
          useDualstackEndpoint: yield config.useDualstackEndpoint()
        })) || {},
        region
      ];
    })).then(([regionInfo, region]) => {
      const { signingRegion, signingService } = regionInfo;
      config.signingRegion = config.signingRegion || signingRegion || region;
      config.signingName = config.signingName || signingService || config.serviceId;
      const params = __spreadProps(__spreadValues({}, config), {
        credentials: config.credentials,
        region: config.signingRegion,
        service: config.signingName,
        sha256,
        uriEscapePath: signingEscapePath
      });
      const SignerCtor = config.signerConstructor || SignatureV4;
      return new SignerCtor(params);
    });
  } else {
    signer = (authScheme) => __async(void 0, null, function* () {
      authScheme = Object.assign({}, {
        name: "sigv4",
        signingName: config.signingName || config.defaultSigningName,
        signingRegion: yield normalizeProvider2(config.region)(),
        properties: {}
      }, authScheme);
      const signingRegion = authScheme.signingRegion;
      const signingService = authScheme.signingName;
      config.signingRegion = config.signingRegion || signingRegion;
      config.signingName = config.signingName || signingService || config.serviceId;
      const params = __spreadProps(__spreadValues({}, config), {
        credentials: config.credentials,
        region: config.signingRegion,
        service: config.signingName,
        sha256,
        uriEscapePath: signingEscapePath
      });
      const SignerCtor = config.signerConstructor || SignatureV4;
      return new SignerCtor(params);
    });
  }
  const resolvedConfig = Object.assign(config, {
    systemClockOffset,
    signingEscapePath,
    signer
  });
  return resolvedConfig;
};
function normalizeCredentialProvider(config, { credentials, credentialDefaultProvider }) {
  let credentialsProvider;
  if (credentials) {
    if (!credentials?.memoized) {
      credentialsProvider = memoizeIdentityProvider(credentials, isIdentityExpired, doesIdentityRequireRefresh);
    } else {
      credentialsProvider = credentials;
    }
  } else {
    if (credentialDefaultProvider) {
      credentialsProvider = normalizeProvider2(credentialDefaultProvider(Object.assign({}, config, {
        parentClientConfig: config
      })));
    } else {
      credentialsProvider = () => __async(this, null, function* () {
        throw new Error("@aws-sdk/core::resolveAwsSdkSigV4Config - `credentials` not provided and no credentialDefaultProvider was configured.");
      });
    }
  }
  credentialsProvider.memoized = true;
  return credentialsProvider;
}
function bindCallerConfig(config, credentialsProvider) {
  if (credentialsProvider.configBound) {
    return credentialsProvider;
  }
  const fn = (options) => __async(this, null, function* () {
    return credentialsProvider(__spreadProps(__spreadValues({}, options), { callerClientConfig: config }));
  });
  fn.memoized = credentialsProvider.memoized;
  fn.configBound = true;
  return fn;
}

// node_modules/@aws-sdk/signature-v4-multi-region/dist-es/signature-v4-crt-container.js
var signatureV4CrtContainer = {
  CrtSignerV4: null
};

// node_modules/@aws-sdk/signature-v4-multi-region/dist-es/SignatureV4MultiRegion.js
var SignatureV4MultiRegion = class {
  sigv4aSigner;
  sigv4Signer;
  signerOptions;
  static sigv4aDependency() {
    if (typeof signatureV4CrtContainer.CrtSignerV4 === "function") {
      return "crt";
    } else if (typeof signatureV4aContainer.SignatureV4a === "function") {
      return "js";
    }
    return "none";
  }
  constructor(options) {
    this.sigv4Signer = new SignatureV4S3Express(options);
    this.signerOptions = options;
  }
  sign(_0) {
    return __async(this, arguments, function* (requestToSign, options = {}) {
      if (options.signingRegion === "*") {
        return this.getSigv4aSigner().sign(requestToSign, options);
      }
      return this.sigv4Signer.sign(requestToSign, options);
    });
  }
  signWithCredentials(_0, _1) {
    return __async(this, arguments, function* (requestToSign, credentials, options = {}) {
      if (options.signingRegion === "*") {
        const signer = this.getSigv4aSigner();
        const CrtSignerV4 = signatureV4CrtContainer.CrtSignerV4;
        if (CrtSignerV4 && signer instanceof CrtSignerV4) {
          return signer.signWithCredentials(requestToSign, credentials, options);
        } else {
          throw new Error(`signWithCredentials with signingRegion '*' is only supported when using the CRT dependency @aws-sdk/signature-v4-crt. Please check whether you have installed the "@aws-sdk/signature-v4-crt" package explicitly. You must also register the package by calling [require("@aws-sdk/signature-v4-crt");] or an ESM equivalent such as [import "@aws-sdk/signature-v4-crt";]. For more information please go to https://github.com/aws/aws-sdk-js-v3#functionality-requiring-aws-common-runtime-crt`);
        }
      }
      return this.sigv4Signer.signWithCredentials(requestToSign, credentials, options);
    });
  }
  presign(_0) {
    return __async(this, arguments, function* (originalRequest, options = {}) {
      if (options.signingRegion === "*") {
        const signer = this.getSigv4aSigner();
        const CrtSignerV4 = signatureV4CrtContainer.CrtSignerV4;
        if (CrtSignerV4 && signer instanceof CrtSignerV4) {
          return signer.presign(originalRequest, options);
        } else {
          throw new Error(`presign with signingRegion '*' is only supported when using the CRT dependency @aws-sdk/signature-v4-crt. Please check whether you have installed the "@aws-sdk/signature-v4-crt" package explicitly. You must also register the package by calling [require("@aws-sdk/signature-v4-crt");] or an ESM equivalent such as [import "@aws-sdk/signature-v4-crt";]. For more information please go to https://github.com/aws/aws-sdk-js-v3#functionality-requiring-aws-common-runtime-crt`);
        }
      }
      return this.sigv4Signer.presign(originalRequest, options);
    });
  }
  presignWithCredentials(_0, _1) {
    return __async(this, arguments, function* (originalRequest, credentials, options = {}) {
      if (options.signingRegion === "*") {
        throw new Error("Method presignWithCredentials is not supported for [signingRegion=*].");
      }
      return this.sigv4Signer.presignWithCredentials(originalRequest, credentials, options);
    });
  }
  getSigv4aSigner() {
    if (!this.sigv4aSigner) {
      const CrtSignerV4 = signatureV4CrtContainer.CrtSignerV4;
      const JsSigV4aSigner = signatureV4aContainer.SignatureV4a;
      if (this.signerOptions.runtime === "node") {
        if (!CrtSignerV4 && !JsSigV4aSigner) {
          throw new Error("Neither CRT nor JS SigV4a implementation is available. Please load either @aws-sdk/signature-v4-crt or @aws-sdk/signature-v4a. For more information please go to https://github.com/aws/aws-sdk-js-v3#functionality-requiring-aws-common-runtime-crt");
        }
        if (CrtSignerV4 && typeof CrtSignerV4 === "function") {
          this.sigv4aSigner = new CrtSignerV4(__spreadProps(__spreadValues({}, this.signerOptions), {
            signingAlgorithm: 1
          }));
        } else if (JsSigV4aSigner && typeof JsSigV4aSigner === "function") {
          this.sigv4aSigner = new JsSigV4aSigner(__spreadValues({}, this.signerOptions));
        } else {
          throw new Error("Available SigV4a implementation is not a valid constructor. Please ensure you've properly imported @aws-sdk/signature-v4-crt or @aws-sdk/signature-v4a.For more information please go to https://github.com/aws/aws-sdk-js-v3#functionality-requiring-aws-common-runtime-crt");
        }
      } else {
        if (!JsSigV4aSigner || typeof JsSigV4aSigner !== "function") {
          throw new Error("JS SigV4a implementation is not available or not a valid constructor. Please check whether you have installed the @aws-sdk/signature-v4a package explicitly. The CRT implementation is not available for browsers. You must also register the package by calling [require('@aws-sdk/signature-v4a');] or an ESM equivalent such as [import '@aws-sdk/signature-v4a';]. For more information please go to https://github.com/aws/aws-sdk-js-v3#using-javascript-non-crt-implementation-of-sigv4a");
        }
        this.sigv4aSigner = new JsSigV4aSigner(__spreadValues({}, this.signerOptions));
      }
    }
    return this.sigv4aSigner;
  }
};

// node_modules/@aws-sdk/client-s3/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/isVirtualHostableS3Bucket.js
var isVirtualHostableS3Bucket2 = (value, allowSubDomains = false) => {
  if (allowSubDomains) {
    for (const label of value.split(".")) {
      if (!isVirtualHostableS3Bucket2(label)) {
        return false;
      }
    }
    return true;
  }
  if (!isValidHostLabel(value)) {
    return false;
  }
  if (value.length < 3 || value.length > 63) {
    return false;
  }
  if (value !== value.toLowerCase()) {
    return false;
  }
  if (isIpAddress(value)) {
    return false;
  }
  return true;
};

// node_modules/@aws-sdk/client-s3/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/parseArn.js
var ARN_DELIMITER2 = ":";
var RESOURCE_DELIMITER2 = "/";
var parseArn2 = (value) => {
  const segments = value.split(ARN_DELIMITER2);
  if (segments.length < 6)
    return null;
  const [arn, partition3, service, region, accountId, ...resourcePath] = segments;
  if (arn !== "arn" || partition3 === "" || service === "" || resourcePath.join(ARN_DELIMITER2) === "")
    return null;
  const resourceId = resourcePath.map((resource) => resource.split(RESOURCE_DELIMITER2)).flat();
  return {
    partition: partition3,
    service,
    region,
    accountId,
    resourceId
  };
};

// node_modules/@aws-sdk/client-s3/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/partitions.json
var partitions_default2 = {
  partitions: [{
    id: "aws",
    outputs: {
      dnsSuffix: "amazonaws.com",
      dualStackDnsSuffix: "api.aws",
      implicitGlobalRegion: "us-east-1",
      name: "aws",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^(us|eu|ap|sa|ca|me|af|il|mx)\\-\\w+\\-\\d+$",
    regions: {
      "af-south-1": {
        description: "Africa (Cape Town)"
      },
      "ap-east-1": {
        description: "Asia Pacific (Hong Kong)"
      },
      "ap-east-2": {
        description: "Asia Pacific (Taipei)"
      },
      "ap-northeast-1": {
        description: "Asia Pacific (Tokyo)"
      },
      "ap-northeast-2": {
        description: "Asia Pacific (Seoul)"
      },
      "ap-northeast-3": {
        description: "Asia Pacific (Osaka)"
      },
      "ap-south-1": {
        description: "Asia Pacific (Mumbai)"
      },
      "ap-south-2": {
        description: "Asia Pacific (Hyderabad)"
      },
      "ap-southeast-1": {
        description: "Asia Pacific (Singapore)"
      },
      "ap-southeast-2": {
        description: "Asia Pacific (Sydney)"
      },
      "ap-southeast-3": {
        description: "Asia Pacific (Jakarta)"
      },
      "ap-southeast-4": {
        description: "Asia Pacific (Melbourne)"
      },
      "ap-southeast-5": {
        description: "Asia Pacific (Malaysia)"
      },
      "ap-southeast-6": {
        description: "Asia Pacific (New Zealand)"
      },
      "ap-southeast-7": {
        description: "Asia Pacific (Thailand)"
      },
      "aws-global": {
        description: "aws global region"
      },
      "ca-central-1": {
        description: "Canada (Central)"
      },
      "ca-west-1": {
        description: "Canada West (Calgary)"
      },
      "eu-central-1": {
        description: "Europe (Frankfurt)"
      },
      "eu-central-2": {
        description: "Europe (Zurich)"
      },
      "eu-north-1": {
        description: "Europe (Stockholm)"
      },
      "eu-south-1": {
        description: "Europe (Milan)"
      },
      "eu-south-2": {
        description: "Europe (Spain)"
      },
      "eu-west-1": {
        description: "Europe (Ireland)"
      },
      "eu-west-2": {
        description: "Europe (London)"
      },
      "eu-west-3": {
        description: "Europe (Paris)"
      },
      "il-central-1": {
        description: "Israel (Tel Aviv)"
      },
      "me-central-1": {
        description: "Middle East (UAE)"
      },
      "me-south-1": {
        description: "Middle East (Bahrain)"
      },
      "mx-central-1": {
        description: "Mexico (Central)"
      },
      "sa-east-1": {
        description: "South America (Sao Paulo)"
      },
      "us-east-1": {
        description: "US East (N. Virginia)"
      },
      "us-east-2": {
        description: "US East (Ohio)"
      },
      "us-west-1": {
        description: "US West (N. California)"
      },
      "us-west-2": {
        description: "US West (Oregon)"
      }
    }
  }, {
    id: "aws-cn",
    outputs: {
      dnsSuffix: "amazonaws.com.cn",
      dualStackDnsSuffix: "api.amazonwebservices.com.cn",
      implicitGlobalRegion: "cn-northwest-1",
      name: "aws-cn",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^cn\\-\\w+\\-\\d+$",
    regions: {
      "aws-cn-global": {
        description: "aws-cn global region"
      },
      "cn-north-1": {
        description: "China (Beijing)"
      },
      "cn-northwest-1": {
        description: "China (Ningxia)"
      }
    }
  }, {
    id: "aws-eusc",
    outputs: {
      dnsSuffix: "amazonaws.eu",
      dualStackDnsSuffix: "api.amazonwebservices.eu",
      implicitGlobalRegion: "eusc-de-east-1",
      name: "aws-eusc",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^eusc\\-(de)\\-\\w+\\-\\d+$",
    regions: {
      "eusc-de-east-1": {
        description: "AWS European Sovereign Cloud (Germany)"
      }
    }
  }, {
    id: "aws-iso",
    outputs: {
      dnsSuffix: "c2s.ic.gov",
      dualStackDnsSuffix: "api.aws.ic.gov",
      implicitGlobalRegion: "us-iso-east-1",
      name: "aws-iso",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-iso\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-global": {
        description: "aws-iso global region"
      },
      "us-iso-east-1": {
        description: "US ISO East"
      },
      "us-iso-west-1": {
        description: "US ISO WEST"
      }
    }
  }, {
    id: "aws-iso-b",
    outputs: {
      dnsSuffix: "sc2s.sgov.gov",
      dualStackDnsSuffix: "api.aws.scloud",
      implicitGlobalRegion: "us-isob-east-1",
      name: "aws-iso-b",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-isob\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-b-global": {
        description: "aws-iso-b global region"
      },
      "us-isob-east-1": {
        description: "US ISOB East (Ohio)"
      },
      "us-isob-west-1": {
        description: "US ISOB West"
      }
    }
  }, {
    id: "aws-iso-e",
    outputs: {
      dnsSuffix: "cloud.adc-e.uk",
      dualStackDnsSuffix: "api.cloud-aws.adc-e.uk",
      implicitGlobalRegion: "eu-isoe-west-1",
      name: "aws-iso-e",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^eu\\-isoe\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-e-global": {
        description: "aws-iso-e global region"
      },
      "eu-isoe-west-1": {
        description: "EU ISOE West"
      }
    }
  }, {
    id: "aws-iso-f",
    outputs: {
      dnsSuffix: "csp.hci.ic.gov",
      dualStackDnsSuffix: "api.aws.hci.ic.gov",
      implicitGlobalRegion: "us-isof-south-1",
      name: "aws-iso-f",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-isof\\-\\w+\\-\\d+$",
    regions: {
      "aws-iso-f-global": {
        description: "aws-iso-f global region"
      },
      "us-isof-east-1": {
        description: "US ISOF EAST"
      },
      "us-isof-south-1": {
        description: "US ISOF SOUTH"
      }
    }
  }, {
    id: "aws-us-gov",
    outputs: {
      dnsSuffix: "amazonaws.com",
      dualStackDnsSuffix: "api.aws",
      implicitGlobalRegion: "us-gov-west-1",
      name: "aws-us-gov",
      supportsDualStack: true,
      supportsFIPS: true
    },
    regionRegex: "^us\\-gov\\-\\w+\\-\\d+$",
    regions: {
      "aws-us-gov-global": {
        description: "aws-us-gov global region"
      },
      "us-gov-east-1": {
        description: "AWS GovCloud (US-East)"
      },
      "us-gov-west-1": {
        description: "AWS GovCloud (US-West)"
      }
    }
  }],
  version: "1.1"
};

// node_modules/@aws-sdk/client-s3/node_modules/@aws-sdk/util-endpoints/dist-es/lib/aws/partition.js
var selectedPartitionsInfo2 = partitions_default2;
var partition2 = (value) => {
  const { partitions } = selectedPartitionsInfo2;
  for (const partition3 of partitions) {
    const { regions, outputs } = partition3;
    for (const [region, regionData] of Object.entries(regions)) {
      if (region === value) {
        return __spreadValues(__spreadValues({}, outputs), regionData);
      }
    }
  }
  for (const partition3 of partitions) {
    const { regionRegex, outputs } = partition3;
    if (new RegExp(regionRegex).test(value)) {
      return __spreadValues({}, outputs);
    }
  }
  const DEFAULT_PARTITION = partitions.find((partition3) => partition3.id === "aws");
  if (!DEFAULT_PARTITION) {
    throw new Error("Provided region was not found in the partition array or regex, and default partition with id 'aws' doesn't exist.");
  }
  return __spreadValues({}, DEFAULT_PARTITION.outputs);
};

// node_modules/@aws-sdk/client-s3/node_modules/@aws-sdk/util-endpoints/dist-es/aws.js
var awsEndpointFunctions2 = {
  isVirtualHostableS3Bucket: isVirtualHostableS3Bucket2,
  parseArn: parseArn2,
  partition: partition2
};
customEndpointFunctions.aws = awsEndpointFunctions2;

// node_modules/@aws-sdk/client-s3/dist-es/endpoint/bdd.js
var aw = "ref";
var ax = "argv";
var ay = "backend";
var az = "authSchemes";
var aA = "disableDoubleEncoding";
var aB = "signingName";
var aC = "signingRegion";
var aD = "signingRegionSet";
var a = -1;
var b = true;
var c = false;
var d = "isSet";
var e = "booleanEquals";
var f = "stringEquals";
var g = "coalesce";
var h = "substring";
var i = "";
var j = "aws.partition";
var k = "partitionResult";
var l = "accessPointSuffix";
var m = "regionPrefix";
var n = (n2) => "outpostId_ssa_" + n2 + i;
var o = "hardwareType";
var p = "ite";
var q = "isValidHostLabel";
var s = "sigv4";
var t = "aws.isVirtualHostableS3Bucket";
var u = "url";
var v = "getAttr";
var w = "bucketArn";
var x = "--";
var y = "arnType";
var z = "accesspoint";
var A = (n2) => "accessPointName_ssa_" + n2 + i;
var B = "s3-object-lambda";
var C = "s3-outposts";
var D = "bucketPartition";
var E = "us-east-1";
var F = "outpostType";
var G = "name";
var H = "s3";
var I = "{url#scheme}://{Bucket}.{url#authority}{url#path}";
var J = "{url#scheme}://{url#authority}{url#path}";
var K = "{url#scheme}://{url#authority}{url#normalizedPath}{Bucket}";
var L = "https://{Bucket}.s3-accelerate.{partitionResult#dnsSuffix}";
var M = "https://{Bucket}.s3.{partitionResult#dnsSuffix}";
var N = (n2) => "{url#scheme}://{accessPointName_ssa_" + n2 + "}-{bucketArn#accountId}.{url#authority}{url#path}";
var O = (n2) => "Invalid ARN: The access point name may only contain a-z, A-Z, 0-9 and `-`. Found: `{accessPointName_ssa_" + n2 + "}`";
var P = "sigv4a";
var Q = "{url#scheme}://{url#authority}{url#normalizedPath}{uri_encoded_bucket}";
var R = "https://s3.{partitionResult#dnsSuffix}/{uri_encoded_bucket}";
var S = "https://s3.{partitionResult#dnsSuffix}";
var T = { [aw]: "UseFIPS" };
var U = { [aw]: "UseDualStack" };
var V = { [aw]: "Bucket" };
var W = { "fn": v, [ax]: [{ [aw]: k }, G] };
var X = { [aw]: u };
var Y = { [aw]: "Region" };
var Z = { [aw]: w };
var aa = { [aw]: y };
var ab = { [aw]: "accessPointName_ssa_1" };
var ac = { "fn": v, [ax]: [Z, "region"] };
var ad = { [aw]: o };
var ae = { "fn": v, [ax]: [Z, "service"] };
var af = { "fn": v, [ax]: [Z, "accountId"] };
var ag = { [ay]: "S3Express", [az]: [{ [aA]: true, [G]: "{_s3e_auth}", [aB]: "s3express", [aC]: "{Region}" }] };
var ah = { [ay]: "S3Express", [az]: [{ [aA]: true, [G]: s, [aB]: "s3express", [aC]: "{Region}" }] };
var ai = { [az]: [{ [aA]: true, [G]: P, [aB]: C, [aD]: ["*"] }, { [aA]: true, [G]: s, [aB]: C, [aC]: "{Region}" }] };
var aj = { [az]: [{ [aA]: true, [G]: s, [aB]: H, [aC]: E }] };
var ak = { [az]: [{ [aA]: true, [G]: s, [aB]: H, [aC]: "{Region}" }] };
var al = { [az]: [{ [aA]: true, [G]: s, [aB]: B, [aC]: "{bucketArn#region}" }] };
var am = { [az]: [{ [aA]: true, [G]: s, [aB]: H, [aC]: "{bucketArn#region}" }] };
var an = { [az]: [{ [aA]: true, [G]: P, [aB]: C, [aD]: ["*"] }, { [aA]: true, [G]: s, [aB]: C, [aC]: "{bucketArn#region}" }] };
var ao = { [az]: [{ [aA]: true, [G]: s, [aB]: B, [aC]: "{Region}" }] };
var ap = [Y];
var aq = [{ [aw]: "Endpoint" }];
var as = [V];
var at = [V, 0, 7, true];
var au = [Z, "resourceId[1]"];
var av = ["*"];
var _data = {
  conditions: [
    [d, ap],
    [e, [{ [aw]: "Accelerate" }, b]],
    [e, [T, b]],
    [e, [U, b]],
    [d, aq],
    [d, as],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 0, 6, b] }, i] }, "--x-s3"]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: at }, i] }, "--xa-s3"]],
    [j, ap, k],
    [h, at, l],
    [f, [{ [aw]: l }, "--op-s3"]],
    [h, [V, 8, 12, b], m],
    [h, [V, 32, 49, b], n(2)],
    [h, [V, 49, 50, b], o],
    [e, [{ [aw]: "ForcePathStyle" }, b]],
    [f, [W, "aws-cn"]],
    [p, [U, ".dualstack", i], "_s3e_ds"],
    [q, [{ [aw]: n(2) }, c]],
    [p, [T, "-fips", i], "_s3e_fips"],
    [p, [{ fn: g, [ax]: [{ [aw]: "DisableS3ExpressSessionAuth" }, c] }, s, "sigv4-s3express"], "_s3e_auth"],
    [t, [V, c]],
    ["parseURL", aq, u],
    [e, [{ fn: g, [ax]: [{ [aw]: "UseS3ExpressControlEndpoint" }, c] }, b]],
    [t, [V, b]],
    [f, [{ fn: v, [ax]: [X, "scheme"] }, "http"]],
    [q, [Y, c]],
    ["aws.parseArn", as, w],
    [v, [{ fn: "split", [ax]: [V, x, 0] }, "[-2]"], "s3expressAvailabilityZoneId"],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 0, 4, c] }, i] }, "arn:"]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 16, 18, b] }, i] }, x]],
    [e, [{ fn: v, [ax]: [X, "isIp"] }, b]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 21, 23, b] }, i] }, x]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 27, 29, b] }, i] }, x]],
    [f, [{ [aw]: m }, "beta"]],
    ["uriEncode", as, "uri_encoded_bucket"],
    [q, [Y, b]],
    [e, [{ fn: g, [ax]: [{ [aw]: "UseObjectLambdaEndpoint" }, c] }, b]],
    [v, [Z, "resourceId[0]"], y],
    [f, [aa, i]],
    [f, [aa, z]],
    [v, au, A(1)],
    [f, [ab, i]],
    [f, [ac, i]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 14, 16, b] }, i] }, x]],
    [f, [ad, "e"]],
    [f, [ad, "o"]],
    [f, [Y, "aws-global"]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 19, 21, b] }, i] }, x]],
    [f, [ae, B]],
    [e, [{ fn: g, [ax]: [{ [aw]: "DisableAccessPoints" }, c] }, b]],
    [f, [ae, C]],
    [j, [ac], D],
    [q, [ab, b]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 26, 28, b] }, i] }, x]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 15, 17, b] }, i] }, x]],
    [v, [Z, "resourceId[4]"]],
    [f, [{ fn: g, [ax]: [{ fn: h, [ax]: [V, 20, 22, b] }, i] }, x]],
    [e, [{ [aw]: "UseGlobalEndpoint" }, b]],
    [f, [Y, E]],
    [v, au, n(1)],
    [e, [{ fn: g, [ax]: [{ [aw]: "UseArnRegion" }, b] }, b]],
    [q, [{ [aw]: n(1) }, c]],
    [v, [Z, "resourceId[2]"], F],
    [f, [Y, ac]],
    [f, [{ fn: v, [ax]: [{ [aw]: D }, G] }, W]],
    [e, [{ [aw]: "DisableMultiRegionAccessPoints" }, b]],
    [q, [ac, b]],
    [f, [{ fn: v, [ax]: [Z, "partition"] }, W]],
    [f, [af, i]],
    [f, [ae, H]],
    [q, [af, c]],
    [v, [Z, "resourceId[3]"], A(2)],
    [q, [ab, c]],
    [f, [{ [aw]: F }, z]],
    [q, [{ [aw]: A(2) }, c]]
  ],
  results: [
    [a],
    [a, "Accelerate cannot be used with FIPS"],
    [a, "Cannot set dual-stack in combination with a custom endpoint."],
    [a, "A custom endpoint cannot be combined with FIPS"],
    [a, "A custom endpoint cannot be combined with S3 Accelerate"],
    [a, "Partition does not support FIPS"],
    [a, "S3Express does not support S3 Accelerate."],
    ["{url#scheme}://{url#authority}/{uri_encoded_bucket}{url#path}", ag],
    [I, ag],
    [a, "S3Express bucket name is not a valid virtual hostable name."],
    ["https://s3express-control{_s3e_fips}{_s3e_ds}.{Region}.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", ah],
    ["https://{Bucket}.s3express{_s3e_fips}-{s3expressAvailabilityZoneId}{_s3e_ds}.{Region}.{partitionResult#dnsSuffix}", ag],
    [a, "Unrecognized S3Express bucket name format."],
    [J, ag],
    ["https://s3express-control{_s3e_fips}{_s3e_ds}.{Region}.{partitionResult#dnsSuffix}", ah],
    [a, "Expected a endpoint to be specified but no endpoint was found"],
    ["https://{Bucket}.ec2.{url#authority}", ai],
    ["https://{Bucket}.ec2.s3-outposts.{Region}.{partitionResult#dnsSuffix}", ai],
    ["https://{Bucket}.op-{outpostId_ssa_2}.{url#authority}", ai],
    ["https://{Bucket}.op-{outpostId_ssa_2}.s3-outposts.{Region}.{partitionResult#dnsSuffix}", ai],
    [a, 'Unrecognized hardware type: "Expected hardware type o or e but got {hardwareType}"'],
    [a, "Invalid Outposts Bucket alias - it must be a valid bucket name."],
    [a, "Invalid ARN: The outpost Id must only contain a-z, A-Z, 0-9 and `-`."],
    [a, "Custom endpoint `{Endpoint}` was not a valid URI"],
    [a, "S3 Accelerate cannot be used in this region"],
    ["https://{Bucket}.s3-fips.dualstack.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://{Bucket}.s3-fips.dualstack.{Region}.{partitionResult#dnsSuffix}", ak],
    ["https://{Bucket}.s3-fips.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://{Bucket}.s3-fips.{Region}.{partitionResult#dnsSuffix}", ak],
    ["https://{Bucket}.s3-accelerate.dualstack.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://{Bucket}.s3-accelerate.dualstack.{partitionResult#dnsSuffix}", ak],
    ["https://{Bucket}.s3.dualstack.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://{Bucket}.s3.dualstack.{Region}.{partitionResult#dnsSuffix}", ak],
    [K, aj],
    [I, aj],
    [K, ak],
    [I, ak],
    [L, aj],
    [L, ak],
    [M, aj],
    [M, ak],
    ["https://{Bucket}.s3.{Region}.{partitionResult#dnsSuffix}", ak],
    [a, "Invalid region: region was not a valid DNS name."],
    [a, "S3 Object Lambda does not support Dual-stack"],
    [a, "S3 Object Lambda does not support S3 Accelerate"],
    [a, "Access points are not supported for this operation"],
    [a, "Invalid configuration: region from ARN `{bucketArn#region}` does not match client region `{Region}` and UseArnRegion is `false`"],
    [a, "Invalid ARN: Missing account id"],
    [N(1), al],
    ["https://{accessPointName_ssa_1}-{bucketArn#accountId}.s3-object-lambda-fips.{bucketArn#region}.{bucketPartition#dnsSuffix}", al],
    ["https://{accessPointName_ssa_1}-{bucketArn#accountId}.s3-object-lambda.{bucketArn#region}.{bucketPartition#dnsSuffix}", al],
    [a, O(1)],
    [a, "Invalid ARN: The account id may only contain a-z, A-Z, 0-9 and `-`. Found: `{bucketArn#accountId}`"],
    [a, "Invalid region in ARN: `{bucketArn#region}` (invalid DNS name)"],
    [a, "Client was configured for partition `{partitionResult#name}` but ARN (`{Bucket}`) has `{bucketPartition#name}`"],
    [a, "Invalid ARN: The ARN may only contain a single resource component after `accesspoint`."],
    [a, "Invalid ARN: bucket ARN is missing a region"],
    [a, "Invalid ARN: Expected a resource of the format `accesspoint:<accesspoint name>` but no name was provided"],
    [a, "Invalid ARN: Object Lambda ARNs only support `accesspoint` arn types, but found: `{arnType}`"],
    [a, "Access Points do not support S3 Accelerate"],
    ["https://{accessPointName_ssa_1}-{bucketArn#accountId}.s3-accesspoint-fips.dualstack.{bucketArn#region}.{bucketPartition#dnsSuffix}", am],
    ["https://{accessPointName_ssa_1}-{bucketArn#accountId}.s3-accesspoint-fips.{bucketArn#region}.{bucketPartition#dnsSuffix}", am],
    ["https://{accessPointName_ssa_1}-{bucketArn#accountId}.s3-accesspoint.dualstack.{bucketArn#region}.{bucketPartition#dnsSuffix}", am],
    [N(1), am],
    ["https://{accessPointName_ssa_1}-{bucketArn#accountId}.s3-accesspoint.{bucketArn#region}.{bucketPartition#dnsSuffix}", am],
    [a, "Invalid ARN: The ARN was not for the S3 service, found: {bucketArn#service}"],
    [a, "S3 MRAP does not support dual-stack"],
    [a, "S3 MRAP does not support FIPS"],
    [a, "S3 MRAP does not support S3 Accelerate"],
    [a, "Invalid configuration: Multi-Region Access Point ARNs are disabled."],
    ["https://{accessPointName_ssa_1}.accesspoint.s3-global.{partitionResult#dnsSuffix}", { [az]: [{ [aA]: b, name: P, [aB]: H, [aD]: av }] }],
    [a, "Client was configured for partition `{partitionResult#name}` but bucket referred to partition `{bucketArn#partition}`"],
    [a, "Invalid Access Point Name"],
    [a, "S3 Outposts does not support Dual-stack"],
    [a, "S3 Outposts does not support FIPS"],
    [a, "S3 Outposts does not support S3 Accelerate"],
    [a, "Invalid Arn: Outpost Access Point ARN contains sub resources"],
    ["https://{accessPointName_ssa_2}-{bucketArn#accountId}.{outpostId_ssa_1}.{url#authority}", an],
    ["https://{accessPointName_ssa_2}-{bucketArn#accountId}.{outpostId_ssa_1}.s3-outposts.{bucketArn#region}.{bucketPartition#dnsSuffix}", an],
    [a, O(2)],
    [a, "Expected an outpost type `accesspoint`, found {outpostType}"],
    [a, "Invalid ARN: expected an access point name"],
    [a, "Invalid ARN: Expected a 4-component resource"],
    [a, "Invalid ARN: The outpost Id may only contain a-z, A-Z, 0-9 and `-`. Found: `{outpostId_ssa_1}`"],
    [a, "Invalid ARN: The Outpost Id was not set"],
    [a, "Invalid ARN: Unrecognized format: {Bucket} (type: {arnType})"],
    [a, "Invalid ARN: No ARN type specified"],
    [a, "Invalid ARN: `{Bucket}` was not a valid ARN"],
    [a, "Path-style addressing cannot be used with ARN buckets"],
    ["https://s3-fips.dualstack.us-east-1.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", aj],
    ["https://s3-fips.dualstack.{Region}.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", ak],
    ["https://s3-fips.us-east-1.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", aj],
    ["https://s3-fips.{Region}.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", ak],
    ["https://s3.dualstack.us-east-1.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", aj],
    ["https://s3.dualstack.{Region}.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", ak],
    [Q, aj],
    [Q, ak],
    [R, aj],
    [R, ak],
    ["https://s3.{Region}.{partitionResult#dnsSuffix}/{uri_encoded_bucket}", ak],
    [a, "Path-style addressing cannot be used with S3 Accelerate"],
    [J, ao],
    ["https://s3-object-lambda-fips.{Region}.{partitionResult#dnsSuffix}", ao],
    ["https://s3-object-lambda.{Region}.{partitionResult#dnsSuffix}", ao],
    ["https://s3-fips.dualstack.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://s3-fips.dualstack.{Region}.{partitionResult#dnsSuffix}", ak],
    ["https://s3-fips.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://s3-fips.{Region}.{partitionResult#dnsSuffix}", ak],
    ["https://s3.dualstack.us-east-1.{partitionResult#dnsSuffix}", aj],
    ["https://s3.dualstack.{Region}.{partitionResult#dnsSuffix}", ak],
    [J, aj],
    [J, ak],
    [S, aj],
    [S, ak],
    ["https://s3.{Region}.{partitionResult#dnsSuffix}", ak],
    [a, "A region must be set when sending requests to S3."]
  ]
};
var root = 2;
var r = 1e8;
var nodes = new Int32Array([
  -1,
  1,
  -1,
  0,
  3,
  r + 115,
  1,
  424,
  4,
  2,
  272,
  5,
  3,
  233,
  6,
  4,
  85,
  7,
  5,
  15,
  8,
  8,
  9,
  r + 115,
  16,
  10,
  13,
  18,
  11,
  13,
  19,
  12,
  13,
  22,
  r + 14,
  13,
  35,
  14,
  r + 42,
  36,
  r + 103,
  435,
  6,
  271,
  16,
  7,
  270,
  17,
  8,
  19,
  18,
  14,
  501,
  106,
  9,
  20,
  24,
  10,
  21,
  24,
  11,
  22,
  24,
  12,
  23,
  24,
  13,
  547,
  24,
  14,
  77,
  25,
  20,
  73,
  26,
  26,
  27,
  78,
  37,
  28,
  r + 86,
  38,
  r + 86,
  29,
  39,
  47,
  30,
  48,
  r + 58,
  31,
  50,
  32,
  r + 85,
  51,
  33,
  136,
  55,
  r + 76,
  34,
  59,
  35,
  r + 84,
  60,
  39,
  36,
  61,
  37,
  r + 83,
  62,
  38,
  146,
  63,
  41,
  r + 46,
  61,
  40,
  r + 83,
  62,
  41,
  150,
  64,
  42,
  r + 54,
  66,
  43,
  r + 53,
  70,
  44,
  r + 52,
  71,
  45,
  r + 81,
  73,
  46,
  r + 80,
  74,
  r + 78,
  r + 79,
  40,
  48,
  r + 57,
  41,
  r + 57,
  49,
  42,
  185,
  50,
  48,
  62,
  51,
  49,
  r + 45,
  52,
  51,
  53,
  526,
  60,
  56,
  54,
  62,
  r + 55,
  55,
  63,
  57,
  r + 46,
  62,
  r + 55,
  57,
  64,
  58,
  r + 54,
  66,
  59,
  r + 53,
  69,
  60,
  r + 65,
  70,
  61,
  r + 52,
  72,
  r + 64,
  r + 51,
  49,
  r + 45,
  63,
  51,
  64,
  526,
  60,
  67,
  65,
  62,
  r + 55,
  66,
  63,
  68,
  r + 46,
  62,
  r + 55,
  68,
  64,
  69,
  r + 54,
  66,
  70,
  r + 53,
  68,
  r + 47,
  71,
  70,
  72,
  r + 52,
  72,
  r + 50,
  r + 51,
  25,
  74,
  r + 42,
  46,
  r + 39,
  75,
  57,
  76,
  r + 41,
  58,
  r + 40,
  r + 41,
  26,
  r + 88,
  78,
  28,
  r + 87,
  79,
  34,
  82,
  80,
  35,
  81,
  545,
  36,
  r + 103,
  r + 115,
  46,
  r + 97,
  83,
  57,
  84,
  r + 99,
  58,
  r + 98,
  r + 99,
  5,
  101,
  86,
  8,
  87,
  r + 115,
  16,
  88,
  89,
  18,
  91,
  89,
  19,
  90,
  92,
  21,
  97,
  95,
  19,
  93,
  92,
  21,
  98,
  95,
  21,
  97,
  94,
  22,
  r + 14,
  95,
  35,
  96,
  r + 42,
  36,
  r + 103,
  r + 42,
  22,
  r + 13,
  98,
  35,
  99,
  r + 42,
  36,
  r + 101,
  100,
  46,
  r + 110,
  r + 111,
  6,
  214,
  102,
  7,
  208,
  103,
  8,
  119,
  104,
  14,
  118,
  105,
  21,
  106,
  r + 23,
  26,
  107,
  502,
  37,
  108,
  r + 86,
  38,
  r + 86,
  109,
  39,
  112,
  110,
  48,
  r + 58,
  111,
  50,
  136,
  r + 85,
  40,
  113,
  r + 57,
  41,
  r + 57,
  114,
  42,
  115,
  500,
  48,
  r + 56,
  116,
  52,
  117,
  r + 72,
  65,
  r + 69,
  r + 72,
  21,
  501,
  r + 23,
  9,
  120,
  124,
  10,
  121,
  124,
  11,
  122,
  124,
  12,
  123,
  124,
  13,
  202,
  124,
  14,
  195,
  125,
  20,
  190,
  126,
  21,
  127,
  r + 23,
  23,
  128,
  129,
  24,
  189,
  129,
  26,
  130,
  197,
  37,
  131,
  r + 86,
  38,
  r + 86,
  132,
  39,
  159,
  133,
  48,
  r + 58,
  134,
  50,
  135,
  r + 85,
  51,
  141,
  136,
  55,
  r + 76,
  137,
  59,
  138,
  r + 84,
  60,
  r + 83,
  139,
  61,
  140,
  r + 83,
  63,
  r + 83,
  r + 46,
  55,
  r + 76,
  142,
  59,
  143,
  r + 84,
  60,
  148,
  144,
  61,
  145,
  r + 83,
  62,
  147,
  146,
  63,
  150,
  r + 46,
  63,
  153,
  r + 46,
  61,
  149,
  r + 83,
  62,
  153,
  150,
  64,
  151,
  r + 54,
  66,
  152,
  r + 53,
  70,
  r + 82,
  r + 52,
  64,
  154,
  r + 54,
  66,
  155,
  r + 53,
  70,
  156,
  r + 52,
  71,
  157,
  r + 81,
  73,
  158,
  r + 80,
  74,
  r + 77,
  r + 79,
  40,
  160,
  r + 57,
  41,
  r + 57,
  161,
  42,
  185,
  162,
  48,
  174,
  163,
  49,
  r + 45,
  164,
  51,
  165,
  526,
  60,
  168,
  166,
  62,
  r + 55,
  167,
  63,
  169,
  r + 46,
  62,
  r + 55,
  169,
  64,
  170,
  r + 54,
  66,
  171,
  r + 53,
  69,
  172,
  r + 65,
  70,
  173,
  r + 52,
  72,
  r + 63,
  r + 51,
  49,
  r + 45,
  175,
  51,
  176,
  526,
  60,
  179,
  177,
  62,
  r + 55,
  178,
  63,
  180,
  r + 46,
  62,
  r + 55,
  180,
  64,
  181,
  r + 54,
  66,
  182,
  r + 53,
  68,
  r + 47,
  183,
  70,
  184,
  r + 52,
  72,
  r + 48,
  r + 51,
  48,
  r + 56,
  186,
  52,
  187,
  r + 72,
  65,
  r + 69,
  188,
  67,
  r + 70,
  r + 71,
  25,
  r + 36,
  r + 42,
  21,
  191,
  r + 23,
  25,
  192,
  r + 42,
  30,
  194,
  193,
  46,
  r + 34,
  r + 36,
  46,
  r + 33,
  r + 35,
  21,
  196,
  r + 23,
  26,
  r + 88,
  197,
  28,
  r + 87,
  198,
  34,
  201,
  199,
  35,
  200,
  545,
  36,
  r + 101,
  r + 115,
  46,
  r + 95,
  r + 96,
  17,
  203,
  r + 22,
  20,
  204,
  r + 21,
  21,
  205,
  550,
  33,
  206,
  550,
  44,
  r + 16,
  207,
  45,
  r + 18,
  r + 20,
  8,
  209,
  215,
  16,
  210,
  220,
  18,
  211,
  220,
  19,
  212,
  224,
  20,
  213,
  227,
  21,
  231,
  401,
  8,
  218,
  215,
  19,
  216,
  r + 9,
  20,
  217,
  227,
  21,
  231,
  r + 9,
  16,
  219,
  220,
  18,
  223,
  220,
  19,
  221,
  224,
  20,
  222,
  227,
  21,
  231,
  r + 12,
  19,
  226,
  224,
  20,
  225,
  r + 9,
  21,
  r + 9,
  r + 12,
  20,
  230,
  227,
  21,
  228,
  r + 9,
  30,
  229,
  r + 9,
  34,
  r + 7,
  r + 9,
  21,
  231,
  415,
  30,
  232,
  r + 8,
  34,
  r + 7,
  r + 8,
  4,
  r + 2,
  234,
  5,
  235,
  480,
  6,
  271,
  236,
  7,
  270,
  237,
  8,
  238,
  491,
  9,
  239,
  243,
  10,
  240,
  243,
  11,
  241,
  243,
  12,
  242,
  243,
  13,
  547,
  243,
  14,
  266,
  244,
  20,
  264,
  245,
  26,
  246,
  267,
  37,
  247,
  r + 86,
  38,
  r + 86,
  248,
  39,
  249,
  518,
  40,
  250,
  r + 57,
  41,
  r + 57,
  251,
  42,
  538,
  252,
  48,
  r + 43,
  253,
  49,
  r + 45,
  254,
  51,
  255,
  526,
  60,
  258,
  256,
  62,
  r + 55,
  257,
  63,
  259,
  r + 46,
  62,
  r + 55,
  259,
  64,
  260,
  r + 54,
  66,
  261,
  r + 53,
  69,
  262,
  r + 65,
  70,
  263,
  r + 52,
  72,
  r + 62,
  r + 51,
  25,
  265,
  r + 42,
  46,
  r + 31,
  r + 32,
  26,
  r + 88,
  267,
  28,
  r + 87,
  268,
  34,
  269,
  544,
  46,
  r + 93,
  r + 94,
  8,
  397,
  r + 9,
  8,
  407,
  r + 9,
  3,
  346,
  273,
  4,
  r + 3,
  274,
  5,
  284,
  275,
  8,
  276,
  r + 115,
  15,
  r + 5,
  277,
  16,
  278,
  281,
  18,
  279,
  281,
  19,
  280,
  281,
  22,
  r + 14,
  281,
  35,
  282,
  r + 42,
  36,
  r + 102,
  283,
  46,
  r + 106,
  r + 107,
  6,
  405,
  285,
  7,
  395,
  286,
  8,
  295,
  287,
  14,
  501,
  288,
  26,
  289,
  502,
  37,
  290,
  r + 86,
  38,
  r + 86,
  291,
  39,
  292,
  307,
  40,
  293,
  r + 57,
  41,
  r + 57,
  294,
  42,
  335,
  500,
  9,
  296,
  300,
  10,
  297,
  300,
  11,
  298,
  300,
  12,
  299,
  300,
  13,
  394,
  300,
  14,
  339,
  301,
  15,
  r + 5,
  302,
  20,
  337,
  303,
  26,
  304,
  341,
  37,
  305,
  r + 86,
  38,
  r + 86,
  306,
  39,
  309,
  307,
  48,
  r + 58,
  308,
  50,
  r + 74,
  r + 85,
  40,
  310,
  r + 57,
  41,
  r + 57,
  311,
  42,
  335,
  312,
  48,
  324,
  313,
  49,
  r + 45,
  314,
  51,
  315,
  526,
  60,
  318,
  316,
  62,
  r + 55,
  317,
  63,
  319,
  r + 46,
  62,
  r + 55,
  319,
  64,
  320,
  r + 54,
  66,
  321,
  r + 53,
  69,
  322,
  r + 65,
  70,
  323,
  r + 52,
  72,
  r + 61,
  r + 51,
  49,
  r + 45,
  325,
  51,
  326,
  526,
  60,
  329,
  327,
  62,
  r + 55,
  328,
  63,
  330,
  r + 46,
  62,
  r + 55,
  330,
  64,
  331,
  r + 54,
  66,
  332,
  r + 53,
  68,
  r + 47,
  333,
  70,
  334,
  r + 52,
  72,
  r + 49,
  r + 51,
  48,
  r + 56,
  336,
  52,
  r + 67,
  r + 72,
  25,
  338,
  r + 42,
  46,
  r + 27,
  r + 28,
  15,
  r + 5,
  340,
  26,
  r + 88,
  341,
  28,
  r + 87,
  342,
  34,
  345,
  343,
  35,
  344,
  545,
  36,
  r + 102,
  r + 115,
  46,
  r + 91,
  r + 92,
  4,
  r + 2,
  347,
  5,
  357,
  348,
  8,
  349,
  r + 115,
  15,
  r + 5,
  350,
  16,
  351,
  354,
  18,
  352,
  354,
  19,
  353,
  354,
  22,
  r + 14,
  354,
  35,
  355,
  r + 42,
  36,
  r + 43,
  356,
  46,
  r + 104,
  r + 105,
  6,
  405,
  358,
  7,
  395,
  359,
  8,
  360,
  491,
  9,
  361,
  365,
  10,
  362,
  365,
  11,
  363,
  365,
  12,
  364,
  365,
  13,
  394,
  365,
  14,
  389,
  366,
  15,
  r + 5,
  367,
  20,
  387,
  368,
  26,
  369,
  391,
  37,
  370,
  r + 86,
  38,
  r + 86,
  371,
  39,
  372,
  518,
  40,
  373,
  r + 57,
  41,
  r + 57,
  374,
  42,
  538,
  375,
  48,
  r + 43,
  376,
  49,
  r + 45,
  377,
  51,
  378,
  526,
  60,
  381,
  379,
  62,
  r + 55,
  380,
  63,
  382,
  r + 46,
  62,
  r + 55,
  382,
  64,
  383,
  r + 54,
  66,
  384,
  r + 53,
  69,
  385,
  r + 65,
  70,
  386,
  r + 52,
  72,
  r + 60,
  r + 51,
  25,
  388,
  r + 42,
  46,
  r + 25,
  r + 26,
  15,
  r + 5,
  390,
  26,
  r + 88,
  391,
  28,
  r + 87,
  392,
  34,
  393,
  544,
  46,
  r + 89,
  r + 90,
  15,
  r + 5,
  547,
  8,
  396,
  r + 9,
  15,
  r + 5,
  397,
  16,
  398,
  410,
  18,
  399,
  410,
  19,
  400,
  410,
  20,
  401,
  r + 9,
  27,
  402,
  r + 12,
  29,
  r + 11,
  403,
  31,
  r + 11,
  404,
  32,
  r + 11,
  422,
  8,
  406,
  r + 9,
  15,
  r + 5,
  407,
  16,
  408,
  410,
  18,
  409,
  410,
  19,
  411,
  410,
  20,
  r + 12,
  r + 9,
  20,
  414,
  412,
  22,
  413,
  r + 9,
  34,
  r + 10,
  r + 9,
  22,
  416,
  415,
  27,
  419,
  r + 12,
  27,
  418,
  417,
  34,
  r + 10,
  r + 12,
  34,
  r + 10,
  419,
  43,
  r + 11,
  420,
  47,
  r + 11,
  421,
  53,
  r + 11,
  422,
  54,
  r + 11,
  423,
  56,
  r + 11,
  r + 12,
  2,
  r + 1,
  425,
  3,
  478,
  426,
  4,
  r + 4,
  427,
  5,
  438,
  428,
  8,
  429,
  r + 115,
  16,
  430,
  433,
  18,
  431,
  433,
  19,
  432,
  433,
  22,
  r + 14,
  433,
  35,
  434,
  r + 42,
  36,
  r + 44,
  435,
  46,
  r + 112,
  436,
  57,
  437,
  r + 114,
  58,
  r + 113,
  r + 114,
  6,
  r + 6,
  439,
  7,
  r + 6,
  440,
  8,
  450,
  441,
  14,
  501,
  442,
  26,
  443,
  502,
  37,
  444,
  r + 86,
  38,
  r + 86,
  445,
  39,
  446,
  465,
  40,
  447,
  r + 57,
  41,
  r + 57,
  448,
  42,
  471,
  449,
  48,
  r + 44,
  500,
  9,
  451,
  455,
  10,
  452,
  455,
  11,
  453,
  455,
  12,
  454,
  455,
  13,
  547,
  455,
  14,
  473,
  456,
  15,
  460,
  457,
  20,
  458,
  461,
  25,
  459,
  r + 42,
  46,
  r + 37,
  r + 38,
  20,
  540,
  461,
  26,
  462,
  474,
  37,
  463,
  r + 86,
  38,
  r + 86,
  464,
  39,
  467,
  465,
  48,
  r + 58,
  466,
  50,
  r + 75,
  r + 85,
  40,
  468,
  r + 57,
  41,
  r + 57,
  469,
  42,
  471,
  470,
  48,
  r + 44,
  524,
  48,
  r + 44,
  472,
  52,
  r + 68,
  r + 72,
  26,
  r + 88,
  474,
  28,
  r + 87,
  475,
  34,
  r + 100,
  476,
  35,
  477,
  545,
  36,
  r + 44,
  r + 115,
  4,
  r + 2,
  479,
  5,
  488,
  480,
  8,
  481,
  r + 115,
  16,
  482,
  485,
  18,
  483,
  485,
  19,
  484,
  485,
  22,
  r + 14,
  485,
  35,
  486,
  r + 42,
  36,
  r + 43,
  487,
  46,
  r + 108,
  r + 109,
  6,
  r + 6,
  489,
  7,
  r + 6,
  490,
  8,
  503,
  491,
  14,
  501,
  492,
  26,
  493,
  502,
  37,
  494,
  r + 86,
  38,
  r + 86,
  495,
  39,
  496,
  518,
  40,
  497,
  r + 57,
  41,
  r + 57,
  498,
  42,
  538,
  499,
  48,
  r + 43,
  500,
  49,
  r + 45,
  526,
  26,
  r + 88,
  502,
  28,
  r + 87,
  r + 115,
  9,
  504,
  508,
  10,
  505,
  508,
  11,
  506,
  508,
  12,
  507,
  508,
  13,
  547,
  508,
  14,
  541,
  509,
  15,
  513,
  510,
  20,
  511,
  514,
  25,
  512,
  r + 42,
  46,
  r + 29,
  r + 30,
  20,
  540,
  514,
  26,
  515,
  542,
  37,
  516,
  r + 86,
  38,
  r + 86,
  517,
  39,
  520,
  518,
  48,
  r + 58,
  519,
  50,
  r + 73,
  r + 85,
  40,
  521,
  r + 57,
  41,
  r + 57,
  522,
  42,
  538,
  523,
  48,
  r + 43,
  524,
  49,
  r + 45,
  525,
  51,
  529,
  526,
  60,
  r + 55,
  527,
  62,
  r + 55,
  528,
  63,
  r + 55,
  r + 46,
  60,
  532,
  530,
  62,
  r + 55,
  531,
  63,
  533,
  r + 46,
  62,
  r + 55,
  533,
  64,
  534,
  r + 54,
  66,
  535,
  r + 53,
  69,
  536,
  r + 65,
  70,
  537,
  r + 52,
  72,
  r + 59,
  r + 51,
  48,
  r + 43,
  539,
  52,
  r + 66,
  r + 72,
  25,
  r + 24,
  r + 42,
  26,
  r + 88,
  542,
  28,
  r + 87,
  543,
  34,
  r + 100,
  544,
  35,
  546,
  545,
  36,
  r + 42,
  r + 115,
  36,
  r + 43,
  r + 115,
  17,
  548,
  r + 22,
  20,
  549,
  r + 21,
  33,
  552,
  550,
  44,
  r + 17,
  551,
  45,
  r + 19,
  r + 20,
  44,
  r + 15,
  553,
  45,
  r + 15,
  r + 20
]);
var bdd = BinaryDecisionDiagram.from(nodes, root, _data.conditions, _data.results);

// node_modules/@aws-sdk/client-s3/dist-es/endpoint/endpointResolver.js
var cache = new EndpointCache({
  size: 50,
  params: [
    "Accelerate",
    "Bucket",
    "DisableAccessPoints",
    "DisableMultiRegionAccessPoints",
    "DisableS3ExpressSessionAuth",
    "Endpoint",
    "ForcePathStyle",
    "Region",
    "UseArnRegion",
    "UseDualStack",
    "UseFIPS",
    "UseGlobalEndpoint",
    "UseObjectLambdaEndpoint",
    "UseS3ExpressControlEndpoint"
  ]
});
var defaultEndpointResolver = (endpointParams, context = {}) => {
  return cache.get(endpointParams, () => decideEndpoint(bdd, {
    endpointParams,
    logger: context.logger
  }));
};
customEndpointFunctions.aws = awsEndpointFunctions2;

// node_modules/@aws-sdk/client-s3/dist-es/auth/httpAuthSchemeProvider.js
var createEndpointRuleSetHttpAuthSchemeParametersProvider = (defaultHttpAuthSchemeParametersProvider) => (config, context, input) => __async(void 0, null, function* () {
  if (!input) {
    throw new Error("Could not find `input` for `defaultEndpointRuleSetHttpAuthSchemeParametersProvider`");
  }
  const defaultParameters = yield defaultHttpAuthSchemeParametersProvider(config, context, input);
  const instructionsFn = getSmithyContext(context)?.commandInstance?.constructor?.getEndpointParameterInstructions;
  if (!instructionsFn) {
    throw new Error(`getEndpointParameterInstructions() is not defined on '${context.commandName}'`);
  }
  const endpointParameters = yield resolveParams(input, { getEndpointParameterInstructions: instructionsFn }, config);
  return Object.assign(defaultParameters, endpointParameters);
});
var _defaultS3HttpAuthSchemeParametersProvider = (config, context, input) => __async(void 0, null, function* () {
  return {
    operation: getSmithyContext(context).operation,
    region: (yield normalizeProvider(config.region)()) || (() => {
      throw new Error("expected `region` to be configured for `aws.auth#sigv4`");
    })()
  };
});
var defaultS3HttpAuthSchemeParametersProvider = createEndpointRuleSetHttpAuthSchemeParametersProvider(_defaultS3HttpAuthSchemeParametersProvider);
function createAwsAuthSigv4HttpAuthOption(authParameters) {
  return {
    schemeId: "aws.auth#sigv4",
    signingProperties: {
      name: "s3",
      region: authParameters.region
    },
    propertiesExtractor: (config, context) => ({
      signingProperties: {
        config,
        context
      }
    })
  };
}
function createAwsAuthSigv4aHttpAuthOption(authParameters) {
  return {
    schemeId: "aws.auth#sigv4a",
    signingProperties: {
      name: "s3",
      region: authParameters.region
    },
    propertiesExtractor: (config, context) => ({
      signingProperties: {
        config,
        context
      }
    })
  };
}
var createEndpointRuleSetHttpAuthSchemeProvider = (defaultEndpointResolver2, defaultHttpAuthSchemeResolver, createHttpAuthOptionFunctions) => {
  const endpointRuleSetHttpAuthSchemeProvider = (authParameters) => {
    const endpoint = defaultEndpointResolver2(authParameters);
    const authSchemes = endpoint.properties?.authSchemes;
    if (!authSchemes) {
      return defaultHttpAuthSchemeResolver(authParameters);
    }
    const options = [];
    for (const scheme of authSchemes) {
      const _a = scheme, { name: resolvedName, properties = {} } = _a, rest = __objRest(_a, ["name", "properties"]);
      const name = resolvedName.toLowerCase();
      if (resolvedName !== name) {
        console.warn(`HttpAuthScheme has been normalized with lowercasing: '${resolvedName}' to '${name}'`);
      }
      let schemeId;
      if (name === "sigv4a") {
        schemeId = "aws.auth#sigv4a";
        const sigv4Present = authSchemes.find((s2) => {
          const name2 = s2.name.toLowerCase();
          return name2 !== "sigv4a" && name2.startsWith("sigv4");
        });
        if (SignatureV4MultiRegion.sigv4aDependency() === "none" && sigv4Present) {
          continue;
        }
      } else if (name.startsWith("sigv4")) {
        schemeId = "aws.auth#sigv4";
      } else {
        throw new Error(`Unknown HttpAuthScheme found in '@smithy.rules#endpointRuleSet': '${name}'`);
      }
      const createOption = createHttpAuthOptionFunctions[schemeId];
      if (!createOption) {
        throw new Error(`Could not find HttpAuthOption create function for '${schemeId}'`);
      }
      const option = createOption(authParameters);
      option.schemeId = schemeId;
      option.signingProperties = __spreadValues(__spreadValues(__spreadValues({}, option.signingProperties || {}), rest), properties);
      options.push(option);
    }
    return options;
  };
  return endpointRuleSetHttpAuthSchemeProvider;
};
var _defaultS3HttpAuthSchemeProvider = (authParameters) => {
  const options = [];
  switch (authParameters.operation) {
    default: {
      options.push(createAwsAuthSigv4HttpAuthOption(authParameters));
      options.push(createAwsAuthSigv4aHttpAuthOption(authParameters));
    }
  }
  return options;
};
var defaultS3HttpAuthSchemeProvider = createEndpointRuleSetHttpAuthSchemeProvider(defaultEndpointResolver, _defaultS3HttpAuthSchemeProvider, {
  "aws.auth#sigv4": createAwsAuthSigv4HttpAuthOption,
  "aws.auth#sigv4a": createAwsAuthSigv4aHttpAuthOption
});
var resolveHttpAuthSchemeConfig = (config) => {
  const config_0 = resolveAwsSdkSigV4Config(config);
  const config_1 = resolveAwsSdkSigV4AConfig(config_0);
  return Object.assign(config_1, {
    authSchemePreference: normalizeProvider(config.authSchemePreference ?? [])
  });
};

// node_modules/@aws-sdk/client-s3/dist-es/endpoint/EndpointParameters.js
var resolveClientEndpointParameters = (options) => {
  return Object.assign(options, {
    useFipsEndpoint: options.useFipsEndpoint ?? false,
    useDualstackEndpoint: options.useDualstackEndpoint ?? false,
    forcePathStyle: options.forcePathStyle ?? false,
    useAccelerateEndpoint: options.useAccelerateEndpoint ?? false,
    useGlobalEndpoint: options.useGlobalEndpoint ?? false,
    disableMultiregionAccessPoints: options.disableMultiregionAccessPoints ?? false,
    defaultSigningName: "s3",
    clientContextParams: options.clientContextParams ?? {}
  });
};
var commonParams = {
  ForcePathStyle: { type: "clientContextParams", name: "forcePathStyle" },
  UseArnRegion: { type: "clientContextParams", name: "useArnRegion" },
  DisableMultiRegionAccessPoints: { type: "clientContextParams", name: "disableMultiregionAccessPoints" },
  Accelerate: { type: "clientContextParams", name: "useAccelerateEndpoint" },
  DisableS3ExpressSessionAuth: { type: "clientContextParams", name: "disableS3ExpressSessionAuth" },
  UseGlobalEndpoint: { type: "builtInParams", name: "useGlobalEndpoint" },
  UseFIPS: { type: "builtInParams", name: "useFipsEndpoint" },
  Endpoint: { type: "builtInParams", name: "endpoint" },
  Region: { type: "builtInParams", name: "region" },
  UseDualStack: { type: "builtInParams", name: "useDualstackEndpoint" }
};

// node_modules/@aws-sdk/client-s3/dist-es/models/S3ServiceException.js
var S3ServiceException = class _S3ServiceException extends ServiceException {
  constructor(options) {
    super(options);
    Object.setPrototypeOf(this, _S3ServiceException.prototype);
  }
};

// node_modules/@aws-sdk/client-s3/dist-es/models/errors.js
var NoSuchUpload = class _NoSuchUpload extends S3ServiceException {
  name = "NoSuchUpload";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "NoSuchUpload",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _NoSuchUpload.prototype);
  }
};
var AccessDenied = class _AccessDenied extends S3ServiceException {
  name = "AccessDenied";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "AccessDenied",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _AccessDenied.prototype);
  }
};
var ObjectNotInActiveTierError = class _ObjectNotInActiveTierError extends S3ServiceException {
  name = "ObjectNotInActiveTierError";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "ObjectNotInActiveTierError",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _ObjectNotInActiveTierError.prototype);
  }
};
var BucketAlreadyExists = class _BucketAlreadyExists extends S3ServiceException {
  name = "BucketAlreadyExists";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "BucketAlreadyExists",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _BucketAlreadyExists.prototype);
  }
};
var BucketAlreadyOwnedByYou = class _BucketAlreadyOwnedByYou extends S3ServiceException {
  name = "BucketAlreadyOwnedByYou";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "BucketAlreadyOwnedByYou",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _BucketAlreadyOwnedByYou.prototype);
  }
};
var NoSuchBucket = class _NoSuchBucket extends S3ServiceException {
  name = "NoSuchBucket";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "NoSuchBucket",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _NoSuchBucket.prototype);
  }
};
var InvalidObjectState = class _InvalidObjectState extends S3ServiceException {
  name = "InvalidObjectState";
  $fault = "client";
  StorageClass;
  AccessTier;
  constructor(opts) {
    super(__spreadValues({
      name: "InvalidObjectState",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _InvalidObjectState.prototype);
    this.StorageClass = opts.StorageClass;
    this.AccessTier = opts.AccessTier;
  }
};
var NoSuchKey = class _NoSuchKey extends S3ServiceException {
  name = "NoSuchKey";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "NoSuchKey",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _NoSuchKey.prototype);
  }
};
var NotFound = class _NotFound extends S3ServiceException {
  name = "NotFound";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "NotFound",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _NotFound.prototype);
  }
};
var EncryptionTypeMismatch = class _EncryptionTypeMismatch extends S3ServiceException {
  name = "EncryptionTypeMismatch";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "EncryptionTypeMismatch",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _EncryptionTypeMismatch.prototype);
  }
};
var InvalidRequest = class _InvalidRequest extends S3ServiceException {
  name = "InvalidRequest";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "InvalidRequest",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _InvalidRequest.prototype);
  }
};
var InvalidWriteOffset = class _InvalidWriteOffset extends S3ServiceException {
  name = "InvalidWriteOffset";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "InvalidWriteOffset",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _InvalidWriteOffset.prototype);
  }
};
var TooManyParts = class _TooManyParts extends S3ServiceException {
  name = "TooManyParts";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "TooManyParts",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _TooManyParts.prototype);
  }
};
var IdempotencyParameterMismatch = class _IdempotencyParameterMismatch extends S3ServiceException {
  name = "IdempotencyParameterMismatch";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "IdempotencyParameterMismatch",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _IdempotencyParameterMismatch.prototype);
  }
};
var ObjectAlreadyInActiveTierError = class _ObjectAlreadyInActiveTierError extends S3ServiceException {
  name = "ObjectAlreadyInActiveTierError";
  $fault = "client";
  constructor(opts) {
    super(__spreadValues({
      name: "ObjectAlreadyInActiveTierError",
      $fault: "client"
    }, opts));
    Object.setPrototypeOf(this, _ObjectAlreadyInActiveTierError.prototype);
  }
};

// node_modules/@aws-sdk/client-s3/dist-es/schemas/schemas_0.js
var _A = "Account";
var _AAO = "AnalyticsAndOperator";
var _AC = "AccelerateConfiguration";
var _ACL = "AccessControlList";
var _ACL_ = "ACL";
var _ACLn = "AnalyticsConfigurationList";
var _ACP = "AccessControlPolicy";
var _ACT = "AccessControlTranslation";
var _ACn = "AnalyticsConfiguration";
var _AD = "AccessDenied";
var _ADb = "AbortDate";
var _AED = "AnalyticsExportDestination";
var _AF = "AnalyticsFilter";
var _AH = "AllowedHeaders";
var _AHl = "AllowedHeader";
var _AI = "AccountId";
var _AIMU = "AbortIncompleteMultipartUpload";
var _AKI = "AccessKeyId";
var _AM = "AllowedMethods";
var _AMU = "AbortMultipartUpload";
var _AMUO = "AbortMultipartUploadOutput";
var _AMUR = "AbortMultipartUploadRequest";
var _AMl = "AllowedMethod";
var _AO = "AllowedOrigins";
var _AOl = "AllowedOrigin";
var _APA = "AccessPointAlias";
var _APAc = "AccessPointArn";
var _AQRD = "AllowQuotedRecordDelimiter";
var _AR = "AcceptRanges";
var _ARI = "AbortRuleId";
var _AS = "AbacStatus";
var _ASBD = "AnalyticsS3BucketDestination";
var _ASSEBD = "ApplyServerSideEncryptionByDefault";
var _ASr = "ArchiveStatus";
var _AT = "AccessTier";
var _An = "And";
var _B = "Bucket";
var _BA = "BucketArn";
var _BAE = "BucketAlreadyExists";
var _BAI = "BucketAccountId";
var _BAOBY = "BucketAlreadyOwnedByYou";
var _BET = "BlockedEncryptionTypes";
var _BGR = "BypassGovernanceRetention";
var _BI = "BucketInfo";
var _BKE = "BucketKeyEnabled";
var _BLC = "BucketLifecycleConfiguration";
var _BLN = "BucketLocationName";
var _BLS = "BucketLoggingStatus";
var _BLT = "BucketLocationType";
var _BN = "BucketNamespace";
var _BNu = "BucketName";
var _BP = "BytesProcessed";
var _BPA = "BlockPublicAcls";
var _BPP = "BlockPublicPolicy";
var _BR = "BucketRegion";
var _BRy = "BytesReturned";
var _BS = "BytesScanned";
var _Bo = "Body";
var _Bu = "Buckets";
var _C = "Checksum";
var _CA = "ChecksumAlgorithm";
var _CACL = "CannedACL";
var _CB = "CreateBucket";
var _CBC = "CreateBucketConfiguration";
var _CBMC = "CreateBucketMetadataConfiguration";
var _CBMCR = "CreateBucketMetadataConfigurationRequest";
var _CBMTC = "CreateBucketMetadataTableConfiguration";
var _CBMTCR = "CreateBucketMetadataTableConfigurationRequest";
var _CBO = "CreateBucketOutput";
var _CBR = "CreateBucketRequest";
var _CC = "CacheControl";
var _CCRC = "ChecksumCRC32";
var _CCRCC = "ChecksumCRC32C";
var _CCRCNVME = "ChecksumCRC64NVME";
var _CC_ = "Cache-Control";
var _CD = "CreationDate";
var _CD_ = "Content-Disposition";
var _CDo = "ContentDisposition";
var _CE = "ContinuationEvent";
var _CE_ = "Content-Encoding";
var _CEo = "ContentEncoding";
var _CF = "CloudFunction";
var _CFC = "CloudFunctionConfiguration";
var _CL = "ContentLanguage";
var _CL_ = "Content-Language";
var _CL__ = "Content-Length";
var _CLo = "ContentLength";
var _CM = "Content-MD5";
var _CMD = "ChecksumMD5";
var _CMDo = "ContentMD5";
var _CMU = "CompletedMultipartUpload";
var _CMUO = "CompleteMultipartUploadOutput";
var _CMUOr = "CreateMultipartUploadOutput";
var _CMUR = "CompleteMultipartUploadResult";
var _CMURo = "CompleteMultipartUploadRequest";
var _CMURr = "CreateMultipartUploadRequest";
var _CMUo = "CompleteMultipartUpload";
var _CMUr = "CreateMultipartUpload";
var _CMh = "ChecksumMode";
var _CO = "CopyObject";
var _COO = "CopyObjectOutput";
var _COR = "CopyObjectResult";
var _CORSC = "CORSConfiguration";
var _CORSR = "CORSRules";
var _CORSRu = "CORSRule";
var _CORo = "CopyObjectRequest";
var _CP = "CommonPrefix";
var _CPL = "CommonPrefixList";
var _CPLo = "CompletedPartList";
var _CPR = "CopyPartResult";
var _CPo = "CompletedPart";
var _CPom = "CommonPrefixes";
var _CR = "ContentRange";
var _CRSBA = "ConfirmRemoveSelfBucketAccess";
var _CR_ = "Content-Range";
var _CS = "CopySource";
var _CSHA = "ChecksumSHA1";
var _CSHAh = "ChecksumSHA256";
var _CSHAhe = "ChecksumSHA512";
var _CSIM = "CopySourceIfMatch";
var _CSIMS = "CopySourceIfModifiedSince";
var _CSINM = "CopySourceIfNoneMatch";
var _CSIUS = "CopySourceIfUnmodifiedSince";
var _CSO = "CreateSessionOutput";
var _CSR = "CreateSessionResult";
var _CSRo = "CopySourceRange";
var _CSRr = "CreateSessionRequest";
var _CSSSECA = "CopySourceSSECustomerAlgorithm";
var _CSSSECK = "CopySourceSSECustomerKey";
var _CSSSECKMD = "CopySourceSSECustomerKeyMD5";
var _CSV = "CSV";
var _CSVI = "CopySourceVersionId";
var _CSVIn = "CSVInput";
var _CSVO = "CSVOutput";
var _CSo = "ConfigurationState";
var _CSr = "CreateSession";
var _CT = "ChecksumType";
var _CT_ = "Content-Type";
var _CTl = "ClientToken";
var _CTo = "ContentType";
var _CTom = "CompressionType";
var _CTon = "ContinuationToken";
var _CXXHASH = "ChecksumXXHASH64";
var _CXXHASHh = "ChecksumXXHASH3";
var _CXXHASHhe = "ChecksumXXHASH128";
var _Co = "Condition";
var _Cod = "Code";
var _Com = "Comments";
var _Con = "Contents";
var _Cont = "Cont";
var _Cr = "Credentials";
var _D = "Days";
var _DAI = "DaysAfterInitiation";
var _DB = "DeleteBucket";
var _DBAC = "DeleteBucketAnalyticsConfiguration";
var _DBACR = "DeleteBucketAnalyticsConfigurationRequest";
var _DBC = "DeleteBucketCors";
var _DBCR = "DeleteBucketCorsRequest";
var _DBE = "DeleteBucketEncryption";
var _DBER = "DeleteBucketEncryptionRequest";
var _DBIC = "DeleteBucketInventoryConfiguration";
var _DBICR = "DeleteBucketInventoryConfigurationRequest";
var _DBITC = "DeleteBucketIntelligentTieringConfiguration";
var _DBITCR = "DeleteBucketIntelligentTieringConfigurationRequest";
var _DBL = "DeleteBucketLifecycle";
var _DBLR = "DeleteBucketLifecycleRequest";
var _DBMC = "DeleteBucketMetadataConfiguration";
var _DBMCR = "DeleteBucketMetadataConfigurationRequest";
var _DBMCRe = "DeleteBucketMetricsConfigurationRequest";
var _DBMCe = "DeleteBucketMetricsConfiguration";
var _DBMTC = "DeleteBucketMetadataTableConfiguration";
var _DBMTCR = "DeleteBucketMetadataTableConfigurationRequest";
var _DBOC = "DeleteBucketOwnershipControls";
var _DBOCR = "DeleteBucketOwnershipControlsRequest";
var _DBP = "DeleteBucketPolicy";
var _DBPR = "DeleteBucketPolicyRequest";
var _DBR = "DeleteBucketRequest";
var _DBRR = "DeleteBucketReplicationRequest";
var _DBRe = "DeleteBucketReplication";
var _DBT = "DeleteBucketTagging";
var _DBTR = "DeleteBucketTaggingRequest";
var _DBW = "DeleteBucketWebsite";
var _DBWR = "DeleteBucketWebsiteRequest";
var _DE = "DataExport";
var _DIM = "DestinationIfMatch";
var _DIMS = "DestinationIfModifiedSince";
var _DINM = "DestinationIfNoneMatch";
var _DIUS = "DestinationIfUnmodifiedSince";
var _DM = "DeleteMarker";
var _DME = "DeleteMarkerEntry";
var _DMR = "DeleteMarkerReplication";
var _DMVI = "DeleteMarkerVersionId";
var _DMe = "DeleteMarkers";
var _DN = "DisplayName";
var _DO = "DeletedObject";
var _DOO = "DeleteObjectOutput";
var _DOOe = "DeleteObjectsOutput";
var _DOR = "DeleteObjectRequest";
var _DORe = "DeleteObjectsRequest";
var _DOT = "DeleteObjectTagging";
var _DOTO = "DeleteObjectTaggingOutput";
var _DOTR = "DeleteObjectTaggingRequest";
var _DOe = "DeletedObjects";
var _DOel = "DeleteObject";
var _DOele = "DeleteObjects";
var _DPAB = "DeletePublicAccessBlock";
var _DPABR = "DeletePublicAccessBlockRequest";
var _DR = "DataRedundancy";
var _DRe = "DefaultRetention";
var _DRel = "DeleteResult";
var _DRes = "DestinationResult";
var _Da = "Date";
var _De = "Delete";
var _Del = "Deleted";
var _Deli = "Delimiter";
var _Des = "Destination";
var _Desc = "Description";
var _Det = "Details";
var _E = "Expiration";
var _EA = "EmailAddress";
var _EBC = "EventBridgeConfiguration";
var _EBO = "ExpectedBucketOwner";
var _EC = "EncryptionConfiguration";
var _ECr = "ErrorCode";
var _ED = "ErrorDetails";
var _EDr = "ErrorDocument";
var _EE = "EndEvent";
var _EH = "ExposeHeaders";
var _EHx = "ExposeHeader";
var _EM = "ErrorMessage";
var _EODM = "ExpiredObjectDeleteMarker";
var _EOR = "ExistingObjectReplication";
var _ES = "ExpiresString";
var _ESBO = "ExpectedSourceBucketOwner";
var _ET = "EncryptionType";
var _ETL = "EncryptionTypeList";
var _ETM = "EncryptionTypeMismatch";
var _ETa = "ETag";
var _ETn = "EncodingType";
var _ETv = "EventThreshold";
var _ETx = "ExpressionType";
var _En = "Encryption";
var _Ena = "Enabled";
var _End = "End";
var _Er = "Errors";
var _Err = "Error";
var _Ev = "Events";
var _Eve = "Event";
var _Ex = "Expires";
var _Exp = "Expression";
var _F = "Filter";
var _FD = "FieldDelimiter";
var _FHI = "FileHeaderInfo";
var _FO = "FetchOwner";
var _FR = "FilterRule";
var _FRL = "FilterRuleList";
var _FRi = "FilterRules";
var _Fi = "Field";
var _Fo = "Format";
var _Fr = "Frequency";
var _G = "Grants";
var _GBA = "GetBucketAbac";
var _GBAC = "GetBucketAccelerateConfiguration";
var _GBACO = "GetBucketAccelerateConfigurationOutput";
var _GBACOe = "GetBucketAnalyticsConfigurationOutput";
var _GBACR = "GetBucketAccelerateConfigurationRequest";
var _GBACRe = "GetBucketAnalyticsConfigurationRequest";
var _GBACe = "GetBucketAnalyticsConfiguration";
var _GBAO = "GetBucketAbacOutput";
var _GBAOe = "GetBucketAclOutput";
var _GBAR = "GetBucketAbacRequest";
var _GBARe = "GetBucketAclRequest";
var _GBAe = "GetBucketAcl";
var _GBC = "GetBucketCors";
var _GBCO = "GetBucketCorsOutput";
var _GBCR = "GetBucketCorsRequest";
var _GBE = "GetBucketEncryption";
var _GBEO = "GetBucketEncryptionOutput";
var _GBER = "GetBucketEncryptionRequest";
var _GBIC = "GetBucketInventoryConfiguration";
var _GBICO = "GetBucketInventoryConfigurationOutput";
var _GBICR = "GetBucketInventoryConfigurationRequest";
var _GBITC = "GetBucketIntelligentTieringConfiguration";
var _GBITCO = "GetBucketIntelligentTieringConfigurationOutput";
var _GBITCR = "GetBucketIntelligentTieringConfigurationRequest";
var _GBL = "GetBucketLocation";
var _GBLC = "GetBucketLifecycleConfiguration";
var _GBLCO = "GetBucketLifecycleConfigurationOutput";
var _GBLCR = "GetBucketLifecycleConfigurationRequest";
var _GBLO = "GetBucketLocationOutput";
var _GBLOe = "GetBucketLoggingOutput";
var _GBLR = "GetBucketLocationRequest";
var _GBLRe = "GetBucketLoggingRequest";
var _GBLe = "GetBucketLogging";
var _GBMC = "GetBucketMetadataConfiguration";
var _GBMCO = "GetBucketMetadataConfigurationOutput";
var _GBMCOe = "GetBucketMetricsConfigurationOutput";
var _GBMCR = "GetBucketMetadataConfigurationResult";
var _GBMCRe = "GetBucketMetadataConfigurationRequest";
var _GBMCRet = "GetBucketMetricsConfigurationRequest";
var _GBMCe = "GetBucketMetricsConfiguration";
var _GBMTC = "GetBucketMetadataTableConfiguration";
var _GBMTCO = "GetBucketMetadataTableConfigurationOutput";
var _GBMTCR = "GetBucketMetadataTableConfigurationResult";
var _GBMTCRe = "GetBucketMetadataTableConfigurationRequest";
var _GBNC = "GetBucketNotificationConfiguration";
var _GBNCR = "GetBucketNotificationConfigurationRequest";
var _GBOC = "GetBucketOwnershipControls";
var _GBOCO = "GetBucketOwnershipControlsOutput";
var _GBOCR = "GetBucketOwnershipControlsRequest";
var _GBP = "GetBucketPolicy";
var _GBPO = "GetBucketPolicyOutput";
var _GBPR = "GetBucketPolicyRequest";
var _GBPS = "GetBucketPolicyStatus";
var _GBPSO = "GetBucketPolicyStatusOutput";
var _GBPSR = "GetBucketPolicyStatusRequest";
var _GBR = "GetBucketReplication";
var _GBRO = "GetBucketReplicationOutput";
var _GBRP = "GetBucketRequestPayment";
var _GBRPO = "GetBucketRequestPaymentOutput";
var _GBRPR = "GetBucketRequestPaymentRequest";
var _GBRR = "GetBucketReplicationRequest";
var _GBT = "GetBucketTagging";
var _GBTO = "GetBucketTaggingOutput";
var _GBTR = "GetBucketTaggingRequest";
var _GBV = "GetBucketVersioning";
var _GBVO = "GetBucketVersioningOutput";
var _GBVR = "GetBucketVersioningRequest";
var _GBW = "GetBucketWebsite";
var _GBWO = "GetBucketWebsiteOutput";
var _GBWR = "GetBucketWebsiteRequest";
var _GFC = "GrantFullControl";
var _GJP = "GlacierJobParameters";
var _GO = "GetObject";
var _GOA = "GetObjectAcl";
var _GOAO = "GetObjectAclOutput";
var _GOAOe = "GetObjectAttributesOutput";
var _GOAP = "GetObjectAttributesParts";
var _GOAR = "GetObjectAclRequest";
var _GOARe = "GetObjectAttributesResponse";
var _GOARet = "GetObjectAttributesRequest";
var _GOAe = "GetObjectAttributes";
var _GOLC = "GetObjectLockConfiguration";
var _GOLCO = "GetObjectLockConfigurationOutput";
var _GOLCR = "GetObjectLockConfigurationRequest";
var _GOLH = "GetObjectLegalHold";
var _GOLHO = "GetObjectLegalHoldOutput";
var _GOLHR = "GetObjectLegalHoldRequest";
var _GOO = "GetObjectOutput";
var _GOR = "GetObjectRequest";
var _GORO = "GetObjectRetentionOutput";
var _GORR = "GetObjectRetentionRequest";
var _GORe = "GetObjectRetention";
var _GOT = "GetObjectTagging";
var _GOTO = "GetObjectTaggingOutput";
var _GOTOe = "GetObjectTorrentOutput";
var _GOTR = "GetObjectTaggingRequest";
var _GOTRe = "GetObjectTorrentRequest";
var _GOTe = "GetObjectTorrent";
var _GPAB = "GetPublicAccessBlock";
var _GPABO = "GetPublicAccessBlockOutput";
var _GPABR = "GetPublicAccessBlockRequest";
var _GR = "GrantRead";
var _GRACP = "GrantReadACP";
var _GW = "GrantWrite";
var _GWACP = "GrantWriteACP";
var _Gr = "Grant";
var _Gra = "Grantee";
var _HB = "HeadBucket";
var _HBO = "HeadBucketOutput";
var _HBR = "HeadBucketRequest";
var _HECRE = "HttpErrorCodeReturnedEquals";
var _HN = "HostName";
var _HO = "HeadObject";
var _HOO = "HeadObjectOutput";
var _HOR = "HeadObjectRequest";
var _HRC = "HttpRedirectCode";
var _I = "Id";
var _IC = "InventoryConfiguration";
var _ICL = "InventoryConfigurationList";
var _ID = "ID";
var _IDn = "IndexDocument";
var _IDnv = "InventoryDestination";
var _IE = "IsEnabled";
var _IEn = "InventoryEncryption";
var _IF = "InventoryFilter";
var _IL = "IsLatest";
var _IM = "IfMatch";
var _IMIT = "IfMatchInitiatedTime";
var _IMLMT = "IfMatchLastModifiedTime";
var _IMS = "IfMatchSize";
var _IMS_ = "If-Modified-Since";
var _IMSf = "IfModifiedSince";
var _IMUR = "InitiateMultipartUploadResult";
var _IM_ = "If-Match";
var _INM = "IfNoneMatch";
var _INM_ = "If-None-Match";
var _IOF = "InventoryOptionalFields";
var _IOS = "InvalidObjectState";
var _IOV = "IncludedObjectVersions";
var _IP = "IsPublic";
var _IPA = "IgnorePublicAcls";
var _IPM = "IdempotencyParameterMismatch";
var _IR = "InvalidRequest";
var _IRIP = "IsRestoreInProgress";
var _IS = "InputSerialization";
var _ISBD = "InventoryS3BucketDestination";
var _ISn = "InventorySchedule";
var _IT = "IsTruncated";
var _ITAO = "IntelligentTieringAndOperator";
var _ITC = "IntelligentTieringConfiguration";
var _ITCL = "IntelligentTieringConfigurationList";
var _ITCR = "InventoryTableConfigurationResult";
var _ITCU = "InventoryTableConfigurationUpdates";
var _ITCn = "InventoryTableConfiguration";
var _ITF = "IntelligentTieringFilter";
var _IUS = "IfUnmodifiedSince";
var _IUS_ = "If-Unmodified-Since";
var _IWO = "InvalidWriteOffset";
var _In = "Initiator";
var _Ini = "Initiated";
var _JSON = "JSON";
var _JSONI = "JSONInput";
var _JSONO = "JSONOutput";
var _JTC = "JournalTableConfiguration";
var _JTCR = "JournalTableConfigurationResult";
var _JTCU = "JournalTableConfigurationUpdates";
var _K = "Key";
var _KC = "KeyCount";
var _KI = "KeyId";
var _KKA = "KmsKeyArn";
var _KM = "KeyMarker";
var _KMSC = "KMSContext";
var _KMSKA = "KMSKeyArn";
var _KMSKI = "KMSKeyId";
var _KMSMKID = "KMSMasterKeyID";
var _KPE = "KeyPrefixEquals";
var _L = "Location";
var _LAMBR = "ListAllMyBucketsResult";
var _LAMDBR = "ListAllMyDirectoryBucketsResult";
var _LB = "ListBuckets";
var _LBAC = "ListBucketAnalyticsConfigurations";
var _LBACO = "ListBucketAnalyticsConfigurationsOutput";
var _LBACR = "ListBucketAnalyticsConfigurationResult";
var _LBACRi = "ListBucketAnalyticsConfigurationsRequest";
var _LBIC = "ListBucketInventoryConfigurations";
var _LBICO = "ListBucketInventoryConfigurationsOutput";
var _LBICR = "ListBucketInventoryConfigurationsRequest";
var _LBITC = "ListBucketIntelligentTieringConfigurations";
var _LBITCO = "ListBucketIntelligentTieringConfigurationsOutput";
var _LBITCR = "ListBucketIntelligentTieringConfigurationsRequest";
var _LBMC = "ListBucketMetricsConfigurations";
var _LBMCO = "ListBucketMetricsConfigurationsOutput";
var _LBMCR = "ListBucketMetricsConfigurationsRequest";
var _LBO = "ListBucketsOutput";
var _LBR = "ListBucketsRequest";
var _LBRi = "ListBucketResult";
var _LC = "LocationConstraint";
var _LCi = "LifecycleConfiguration";
var _LDB = "ListDirectoryBuckets";
var _LDBO = "ListDirectoryBucketsOutput";
var _LDBR = "ListDirectoryBucketsRequest";
var _LE = "LoggingEnabled";
var _LEi = "LifecycleExpiration";
var _LFA = "LambdaFunctionArn";
var _LFC = "LambdaFunctionConfiguration";
var _LFCL = "LambdaFunctionConfigurationList";
var _LFCa = "LambdaFunctionConfigurations";
var _LH = "LegalHold";
var _LI = "LocationInfo";
var _LICR = "ListInventoryConfigurationsResult";
var _LM = "LastModified";
var _LMCR = "ListMetricsConfigurationsResult";
var _LMT = "LastModifiedTime";
var _LMU = "ListMultipartUploads";
var _LMUO = "ListMultipartUploadsOutput";
var _LMUR = "ListMultipartUploadsResult";
var _LMURi = "ListMultipartUploadsRequest";
var _LM_ = "Last-Modified";
var _LO = "ListObjects";
var _LOO = "ListObjectsOutput";
var _LOR = "ListObjectsRequest";
var _LOV = "ListObjectsV2";
var _LOVO = "ListObjectsV2Output";
var _LOVOi = "ListObjectVersionsOutput";
var _LOVR = "ListObjectsV2Request";
var _LOVRi = "ListObjectVersionsRequest";
var _LOVi = "ListObjectVersions";
var _LP = "ListParts";
var _LPO = "ListPartsOutput";
var _LPR = "ListPartsResult";
var _LPRi = "ListPartsRequest";
var _LR = "LifecycleRule";
var _LRAO = "LifecycleRuleAndOperator";
var _LRF = "LifecycleRuleFilter";
var _LRi = "LifecycleRules";
var _LVR = "ListVersionsResult";
var _M = "Metadata";
var _MAO = "MetricsAndOperator";
var _MAS = "MaxAgeSeconds";
var _MB = "MaxBuckets";
var _MC = "MetadataConfiguration";
var _MCL = "MetricsConfigurationList";
var _MCR = "MetadataConfigurationResult";
var _MCe = "MetricsConfiguration";
var _MD = "MetadataDirective";
var _MDB = "MaxDirectoryBuckets";
var _MDf = "MfaDelete";
var _ME = "MetadataEntry";
var _MF = "MetricsFilter";
var _MFA = "MFA";
var _MFAD = "MFADelete";
var _MK = "MaxKeys";
var _MM = "MissingMeta";
var _MOS = "MpuObjectSize";
var _MP = "MaxParts";
var _MTC = "MetadataTableConfiguration";
var _MTCR = "MetadataTableConfigurationResult";
var _MTEC = "MetadataTableEncryptionConfiguration";
var _MU = "MultipartUpload";
var _MUL = "MultipartUploadList";
var _MUa = "MaxUploads";
var _Ma = "Marker";
var _Me = "Metrics";
var _Mes = "Message";
var _Mi = "Minutes";
var _Mo = "Mode";
var _N = "Name";
var _NC = "NotificationConfiguration";
var _NCF = "NotificationConfigurationFilter";
var _NCT = "NextContinuationToken";
var _ND = "NoncurrentDays";
var _NEKKAS = "NonEmptyKmsKeyArnString";
var _NF = "NotFound";
var _NKM = "NextKeyMarker";
var _NM = "NextMarker";
var _NNV = "NewerNoncurrentVersions";
var _NPNM = "NextPartNumberMarker";
var _NSB = "NoSuchBucket";
var _NSK = "NoSuchKey";
var _NSU = "NoSuchUpload";
var _NUIM = "NextUploadIdMarker";
var _NVE = "NoncurrentVersionExpiration";
var _NVIM = "NextVersionIdMarker";
var _NVT = "NoncurrentVersionTransitions";
var _NVTL = "NoncurrentVersionTransitionList";
var _NVTo = "NoncurrentVersionTransition";
var _O = "Owner";
var _OA = "ObjectAttributes";
var _OAIATE = "ObjectAlreadyInActiveTierError";
var _OC = "OwnershipControls";
var _OCR = "OwnershipControlsRule";
var _OCRw = "OwnershipControlsRules";
var _OE = "ObjectEncryption";
var _OF = "OptionalFields";
var _OI = "ObjectIdentifier";
var _OIL = "ObjectIdentifierList";
var _OL = "OutputLocation";
var _OLC = "ObjectLockConfiguration";
var _OLE = "ObjectLockEnabled";
var _OLEFB = "ObjectLockEnabledForBucket";
var _OLLH = "ObjectLockLegalHold";
var _OLLHS = "ObjectLockLegalHoldStatus";
var _OLM = "ObjectLockMode";
var _OLR = "ObjectLockRetention";
var _OLRUD = "ObjectLockRetainUntilDate";
var _OLRb = "ObjectLockRule";
var _OLb = "ObjectList";
var _ONIATE = "ObjectNotInActiveTierError";
var _OO = "ObjectOwnership";
var _OOA = "OptionalObjectAttributes";
var _OP = "ObjectParts";
var _OPb = "ObjectPart";
var _OS = "ObjectSize";
var _OSGT = "ObjectSizeGreaterThan";
var _OSLT = "ObjectSizeLessThan";
var _OSV = "OutputSchemaVersion";
var _OSu = "OutputSerialization";
var _OV = "ObjectVersion";
var _OVL = "ObjectVersionList";
var _Ob = "Objects";
var _Obj = "Object";
var _P = "Prefix";
var _PABC = "PublicAccessBlockConfiguration";
var _PBA = "PutBucketAbac";
var _PBAC = "PutBucketAccelerateConfiguration";
var _PBACR = "PutBucketAccelerateConfigurationRequest";
var _PBACRu = "PutBucketAnalyticsConfigurationRequest";
var _PBACu = "PutBucketAnalyticsConfiguration";
var _PBAR = "PutBucketAbacRequest";
var _PBARu = "PutBucketAclRequest";
var _PBAu = "PutBucketAcl";
var _PBC = "PutBucketCors";
var _PBCR = "PutBucketCorsRequest";
var _PBE = "PutBucketEncryption";
var _PBER = "PutBucketEncryptionRequest";
var _PBIC = "PutBucketInventoryConfiguration";
var _PBICR = "PutBucketInventoryConfigurationRequest";
var _PBITC = "PutBucketIntelligentTieringConfiguration";
var _PBITCR = "PutBucketIntelligentTieringConfigurationRequest";
var _PBL = "PutBucketLogging";
var _PBLC = "PutBucketLifecycleConfiguration";
var _PBLCO = "PutBucketLifecycleConfigurationOutput";
var _PBLCR = "PutBucketLifecycleConfigurationRequest";
var _PBLR = "PutBucketLoggingRequest";
var _PBMC = "PutBucketMetricsConfiguration";
var _PBMCR = "PutBucketMetricsConfigurationRequest";
var _PBNC = "PutBucketNotificationConfiguration";
var _PBNCR = "PutBucketNotificationConfigurationRequest";
var _PBOC = "PutBucketOwnershipControls";
var _PBOCR = "PutBucketOwnershipControlsRequest";
var _PBP = "PutBucketPolicy";
var _PBPR = "PutBucketPolicyRequest";
var _PBR = "PutBucketReplication";
var _PBRP = "PutBucketRequestPayment";
var _PBRPR = "PutBucketRequestPaymentRequest";
var _PBRR = "PutBucketReplicationRequest";
var _PBT = "PutBucketTagging";
var _PBTR = "PutBucketTaggingRequest";
var _PBV = "PutBucketVersioning";
var _PBVR = "PutBucketVersioningRequest";
var _PBW = "PutBucketWebsite";
var _PBWR = "PutBucketWebsiteRequest";
var _PC = "PartsCount";
var _PDS = "PartitionDateSource";
var _PE = "ProgressEvent";
var _PI = "ParquetInput";
var _PL = "PartsList";
var _PN = "PartNumber";
var _PNM = "PartNumberMarker";
var _PO = "PutObject";
var _POA = "PutObjectAcl";
var _POAO = "PutObjectAclOutput";
var _POAR = "PutObjectAclRequest";
var _POLC = "PutObjectLockConfiguration";
var _POLCO = "PutObjectLockConfigurationOutput";
var _POLCR = "PutObjectLockConfigurationRequest";
var _POLH = "PutObjectLegalHold";
var _POLHO = "PutObjectLegalHoldOutput";
var _POLHR = "PutObjectLegalHoldRequest";
var _POO = "PutObjectOutput";
var _POR = "PutObjectRequest";
var _PORO = "PutObjectRetentionOutput";
var _PORR = "PutObjectRetentionRequest";
var _PORu = "PutObjectRetention";
var _POT = "PutObjectTagging";
var _POTO = "PutObjectTaggingOutput";
var _POTR = "PutObjectTaggingRequest";
var _PP = "PartitionedPrefix";
var _PPAB = "PutPublicAccessBlock";
var _PPABR = "PutPublicAccessBlockRequest";
var _PS = "PolicyStatus";
var _Pa = "Parts";
var _Par = "Part";
var _Parq = "Parquet";
var _Pay = "Payer";
var _Payl = "Payload";
var _Pe = "Permission";
var _Po = "Policy";
var _Pr = "Progress";
var _Pri = "Priority";
var _Pro = "Protocol";
var _Q = "Quiet";
var _QA = "QueueArn";
var _QC = "QuoteCharacter";
var _QCL = "QueueConfigurationList";
var _QCu = "QueueConfigurations";
var _QCue = "QueueConfiguration";
var _QEC = "QuoteEscapeCharacter";
var _QF = "QuoteFields";
var _Qu = "Queue";
var _R = "Rules";
var _RART = "RedirectAllRequestsTo";
var _RC = "RequestCharged";
var _RCC = "ResponseCacheControl";
var _RCD = "ResponseContentDisposition";
var _RCE = "ResponseContentEncoding";
var _RCL = "ResponseContentLanguage";
var _RCT = "ResponseContentType";
var _RCe = "ReplicationConfiguration";
var _RD = "RecordDelimiter";
var _RE = "ResponseExpires";
var _RED = "RestoreExpiryDate";
var _REe = "RecordExpiration";
var _REec = "RecordsEvent";
var _RKKID = "ReplicaKmsKeyID";
var _RKPW = "ReplaceKeyPrefixWith";
var _RKW = "ReplaceKeyWith";
var _RM = "ReplicaModifications";
var _RO = "RenameObject";
var _ROO = "RenameObjectOutput";
var _ROOe = "RestoreObjectOutput";
var _ROP = "RestoreOutputPath";
var _ROR = "RenameObjectRequest";
var _RORe = "RestoreObjectRequest";
var _ROe = "RestoreObject";
var _RP = "RequestPayer";
var _RPB = "RestrictPublicBuckets";
var _RPC = "RequestPaymentConfiguration";
var _RPe = "RequestProgress";
var _RR = "RoutingRules";
var _RRAO = "ReplicationRuleAndOperator";
var _RRF = "ReplicationRuleFilter";
var _RRe = "ReplicationRule";
var _RRep = "ReplicationRules";
var _RReq = "RequestRoute";
var _RRes = "RestoreRequest";
var _RRo = "RoutingRule";
var _RS = "ReplicationStatus";
var _RSe = "RestoreStatus";
var _RSen = "RenameSource";
var _RT = "ReplicationTime";
var _RTV = "ReplicationTimeValue";
var _RTe = "RequestToken";
var _RUD = "RetainUntilDate";
var _Ra = "Range";
var _Re = "Restore";
var _Rec = "Records";
var _Red = "Redirect";
var _Ret = "Retention";
var _Ro = "Role";
var _Ru = "Rule";
var _S = "Status";
var _SA = "StartAfter";
var _SAK = "SecretAccessKey";
var _SAs = "SseAlgorithm";
var _SB = "StreamingBlob";
var _SBD = "S3BucketDestination";
var _SC = "StorageClass";
var _SCA = "StorageClassAnalysis";
var _SCADE = "StorageClassAnalysisDataExport";
var _SCV = "SessionCredentialValue";
var _SCe = "SessionCredentials";
var _SCt = "StatusCode";
var _SDV = "SkipDestinationValidation";
var _SE = "StatsEvent";
var _SIM = "SourceIfMatch";
var _SIMS = "SourceIfModifiedSince";
var _SINM = "SourceIfNoneMatch";
var _SIUS = "SourceIfUnmodifiedSince";
var _SK = "SSE-KMS";
var _SKEO = "SseKmsEncryptedObjects";
var _SKF = "S3KeyFilter";
var _SKe = "S3Key";
var _SL = "S3Location";
var _SM = "SessionMode";
var _SOC = "SelectObjectContent";
var _SOCES = "SelectObjectContentEventStream";
var _SOCO = "SelectObjectContentOutput";
var _SOCR = "SelectObjectContentRequest";
var _SP = "SelectParameters";
var _SPi = "SimplePrefix";
var _SR = "ScanRange";
var _SS = "SSE-S3";
var _SSC = "SourceSelectionCriteria";
var _SSE = "ServerSideEncryption";
var _SSEA = "SSEAlgorithm";
var _SSEBD = "ServerSideEncryptionByDefault";
var _SSEC = "ServerSideEncryptionConfiguration";
var _SSECA = "SSECustomerAlgorithm";
var _SSECK = "SSECustomerKey";
var _SSECKMD = "SSECustomerKeyMD5";
var _SSEKMS = "SSEKMS";
var _SSEKMSE = "SSEKMSEncryption";
var _SSEKMSEC = "SSEKMSEncryptionContext";
var _SSEKMSKI = "SSEKMSKeyId";
var _SSER = "ServerSideEncryptionRule";
var _SSERe = "ServerSideEncryptionRules";
var _SSES = "SSES3";
var _ST = "SessionToken";
var _STD = "S3TablesDestination";
var _STDR = "S3TablesDestinationResult";
var _S_ = "S3";
var _Sc = "Schedule";
var _Si = "Size";
var _St = "Start";
var _Sta = "Stats";
var _Su = "Suffix";
var _T = "Tags";
var _TA = "TableArn";
var _TAo = "TopicArn";
var _TB = "TargetBucket";
var _TBA = "TableBucketArn";
var _TBT = "TableBucketType";
var _TC = "TagCount";
var _TCL = "TopicConfigurationList";
var _TCo = "TopicConfigurations";
var _TCop = "TopicConfiguration";
var _TD = "TaggingDirective";
var _TDMOS = "TransitionDefaultMinimumObjectSize";
var _TG = "TargetGrants";
var _TGa = "TargetGrant";
var _TL = "TieringList";
var _TLr = "TransitionList";
var _TMP = "TooManyParts";
var _TN = "TableNamespace";
var _TNa = "TableName";
var _TOKF = "TargetObjectKeyFormat";
var _TP = "TargetPrefix";
var _TPC = "TotalPartsCount";
var _TS = "TagSet";
var _TSa = "TableStatus";
var _Ta = "Tag";
var _Tag = "Tagging";
var _Ti = "Tier";
var _Tie = "Tierings";
var _Tier = "Tiering";
var _Tim = "Time";
var _To = "Token";
var _Top = "Topic";
var _Tr = "Transitions";
var _Tra = "Transition";
var _Ty = "Type";
var _U = "Uploads";
var _UBMITC = "UpdateBucketMetadataInventoryTableConfiguration";
var _UBMITCR = "UpdateBucketMetadataInventoryTableConfigurationRequest";
var _UBMJTC = "UpdateBucketMetadataJournalTableConfiguration";
var _UBMJTCR = "UpdateBucketMetadataJournalTableConfigurationRequest";
var _UI = "UploadId";
var _UIM = "UploadIdMarker";
var _UM = "UserMetadata";
var _UOE = "UpdateObjectEncryption";
var _UOER = "UpdateObjectEncryptionRequest";
var _UOERp = "UpdateObjectEncryptionResponse";
var _UP = "UploadPart";
var _UPC = "UploadPartCopy";
var _UPCO = "UploadPartCopyOutput";
var _UPCR = "UploadPartCopyRequest";
var _UPO = "UploadPartOutput";
var _UPR = "UploadPartRequest";
var _URI = "URI";
var _Up = "Upload";
var _V = "Value";
var _VC = "VersioningConfiguration";
var _VI = "VersionId";
var _VIM = "VersionIdMarker";
var _Ve = "Versions";
var _Ver = "Version";
var _WC = "WebsiteConfiguration";
var _WGOR = "WriteGetObjectResponse";
var _WGORR = "WriteGetObjectResponseRequest";
var _WOB = "WriteOffsetBytes";
var _WRL = "WebsiteRedirectLocation";
var _Y = "Years";
var _ar = "accept-ranges";
var _br = "bucket-region";
var _c = "client";
var _ct = "continuation-token";
var _d = "delimiter";
var _e = "error";
var _eP = "eventPayload";
var _en = "endpoint";
var _et = "encoding-type";
var _fo = "fetch-owner";
var _h = "http";
var _hC = "httpChecksum";
var _hE = "httpError";
var _hH = "httpHeader";
var _hL = "hostLabel";
var _hP = "httpPayload";
var _hPH = "httpPrefixHeaders";
var _hQ = "httpQuery";
var _hi = "http://www.w3.org/2001/XMLSchema-instance";
var _i = "id";
var _iT = "idempotencyToken";
var _km = "key-marker";
var _m = "marker";
var _mb = "max-buckets";
var _mdb = "max-directory-buckets";
var _mk = "max-keys";
var _mp = "max-parts";
var _mu = "max-uploads";
var _p = "prefix";
var _pN = "partNumber";
var _pnm = "part-number-marker";
var _rcc = "response-cache-control";
var _rcd = "response-content-disposition";
var _rce = "response-content-encoding";
var _rcl = "response-content-language";
var _rct = "response-content-type";
var _re = "response-expires";
var _s = "smithy.ts.sdk.synthetic.com.amazonaws.s3";
var _sa = "start-after";
var _st = "streaming";
var _uI = "uploadId";
var _uim = "upload-id-marker";
var _vI = "versionId";
var _vim = "version-id-marker";
var _x = "xsi";
var _xA = "xmlAttribute";
var _xF = "xmlFlattened";
var _xN = "xmlName";
var _xNm = "xmlNamespace";
var _xaa = "x-amz-acl";
var _xaad = "x-amz-abort-date";
var _xaapa = "x-amz-access-point-alias";
var _xaari = "x-amz-abort-rule-id";
var _xaas = "x-amz-archive-status";
var _xaba = "x-amz-bucket-arn";
var _xabgr = "x-amz-bypass-governance-retention";
var _xabln = "x-amz-bucket-location-name";
var _xablt = "x-amz-bucket-location-type";
var _xabn = "x-amz-bucket-namespace";
var _xabole = "x-amz-bucket-object-lock-enabled";
var _xabolt = "x-amz-bucket-object-lock-token";
var _xabr = "x-amz-bucket-region";
var _xaca = "x-amz-checksum-algorithm";
var _xacc = "x-amz-checksum-crc32";
var _xacc_ = "x-amz-checksum-crc32c";
var _xacc__ = "x-amz-checksum-crc64nvme";
var _xacm = "x-amz-checksum-md5";
var _xacm_ = "x-amz-checksum-mode";
var _xacrsba = "x-amz-confirm-remove-self-bucket-access";
var _xacs = "x-amz-checksum-sha1";
var _xacs_ = "x-amz-checksum-sha256";
var _xacs__ = "x-amz-checksum-sha512";
var _xacs___ = "x-amz-copy-source";
var _xacsim = "x-amz-copy-source-if-match";
var _xacsims = "x-amz-copy-source-if-modified-since";
var _xacsinm = "x-amz-copy-source-if-none-match";
var _xacsius = "x-amz-copy-source-if-unmodified-since";
var _xacsm = "x-amz-create-session-mode";
var _xacsr = "x-amz-copy-source-range";
var _xacssseca = "x-amz-copy-source-server-side-encryption-customer-algorithm";
var _xacssseck = "x-amz-copy-source-server-side-encryption-customer-key";
var _xacssseckM = "x-amz-copy-source-server-side-encryption-customer-key-MD5";
var _xacsvi = "x-amz-copy-source-version-id";
var _xact = "x-amz-checksum-type";
var _xact_ = "x-amz-client-token";
var _xacx = "x-amz-checksum-xxhash64";
var _xacx_ = "x-amz-checksum-xxhash3";
var _xacx__ = "x-amz-checksum-xxhash128";
var _xadm = "x-amz-delete-marker";
var _xae = "x-amz-expiration";
var _xaebo = "x-amz-expected-bucket-owner";
var _xafec = "x-amz-fwd-error-code";
var _xafem = "x-amz-fwd-error-message";
var _xafhCC = "x-amz-fwd-header-Cache-Control";
var _xafhCD = "x-amz-fwd-header-Content-Disposition";
var _xafhCE = "x-amz-fwd-header-Content-Encoding";
var _xafhCL = "x-amz-fwd-header-Content-Language";
var _xafhCR = "x-amz-fwd-header-Content-Range";
var _xafhCT = "x-amz-fwd-header-Content-Type";
var _xafhE = "x-amz-fwd-header-ETag";
var _xafhE_ = "x-amz-fwd-header-Expires";
var _xafhLM = "x-amz-fwd-header-Last-Modified";
var _xafhar = "x-amz-fwd-header-accept-ranges";
var _xafhxacc = "x-amz-fwd-header-x-amz-checksum-crc32";
var _xafhxacc_ = "x-amz-fwd-header-x-amz-checksum-crc32c";
var _xafhxacc__ = "x-amz-fwd-header-x-amz-checksum-crc64nvme";
var _xafhxacm = "x-amz-fwd-header-x-amz-checksum-md5";
var _xafhxacs = "x-amz-fwd-header-x-amz-checksum-sha1";
var _xafhxacs_ = "x-amz-fwd-header-x-amz-checksum-sha256";
var _xafhxacs__ = "x-amz-fwd-header-x-amz-checksum-sha512";
var _xafhxacx = "x-amz-fwd-header-x-amz-checksum-xxhash64";
var _xafhxacx_ = "x-amz-fwd-header-x-amz-checksum-xxhash3";
var _xafhxacx__ = "x-amz-fwd-header-x-amz-checksum-xxhash128";
var _xafhxadm = "x-amz-fwd-header-x-amz-delete-marker";
var _xafhxae = "x-amz-fwd-header-x-amz-expiration";
var _xafhxamm = "x-amz-fwd-header-x-amz-missing-meta";
var _xafhxampc = "x-amz-fwd-header-x-amz-mp-parts-count";
var _xafhxaollh = "x-amz-fwd-header-x-amz-object-lock-legal-hold";
var _xafhxaolm = "x-amz-fwd-header-x-amz-object-lock-mode";
var _xafhxaolrud = "x-amz-fwd-header-x-amz-object-lock-retain-until-date";
var _xafhxar = "x-amz-fwd-header-x-amz-restore";
var _xafhxarc = "x-amz-fwd-header-x-amz-request-charged";
var _xafhxars = "x-amz-fwd-header-x-amz-replication-status";
var _xafhxasc = "x-amz-fwd-header-x-amz-storage-class";
var _xafhxasse = "x-amz-fwd-header-x-amz-server-side-encryption";
var _xafhxasseakki = "x-amz-fwd-header-x-amz-server-side-encryption-aws-kms-key-id";
var _xafhxassebke = "x-amz-fwd-header-x-amz-server-side-encryption-bucket-key-enabled";
var _xafhxasseca = "x-amz-fwd-header-x-amz-server-side-encryption-customer-algorithm";
var _xafhxasseckM = "x-amz-fwd-header-x-amz-server-side-encryption-customer-key-MD5";
var _xafhxatc = "x-amz-fwd-header-x-amz-tagging-count";
var _xafhxavi = "x-amz-fwd-header-x-amz-version-id";
var _xafs = "x-amz-fwd-status";
var _xagfc = "x-amz-grant-full-control";
var _xagr = "x-amz-grant-read";
var _xagra = "x-amz-grant-read-acp";
var _xagw = "x-amz-grant-write";
var _xagwa = "x-amz-grant-write-acp";
var _xaimit = "x-amz-if-match-initiated-time";
var _xaimlmt = "x-amz-if-match-last-modified-time";
var _xaims = "x-amz-if-match-size";
var _xam = "x-amz-meta-";
var _xam_ = "x-amz-mfa";
var _xamd = "x-amz-metadata-directive";
var _xamm = "x-amz-missing-meta";
var _xamos = "x-amz-mp-object-size";
var _xamp = "x-amz-max-parts";
var _xampc = "x-amz-mp-parts-count";
var _xaoa = "x-amz-object-attributes";
var _xaollh = "x-amz-object-lock-legal-hold";
var _xaolm = "x-amz-object-lock-mode";
var _xaolrud = "x-amz-object-lock-retain-until-date";
var _xaoo = "x-amz-object-ownership";
var _xaooa = "x-amz-optional-object-attributes";
var _xaos = "x-amz-object-size";
var _xapnm = "x-amz-part-number-marker";
var _xar = "x-amz-restore";
var _xarc = "x-amz-request-charged";
var _xarop = "x-amz-restore-output-path";
var _xarp = "x-amz-request-payer";
var _xarr = "x-amz-request-route";
var _xars = "x-amz-replication-status";
var _xars_ = "x-amz-rename-source";
var _xarsim = "x-amz-rename-source-if-match";
var _xarsims = "x-amz-rename-source-if-modified-since";
var _xarsinm = "x-amz-rename-source-if-none-match";
var _xarsius = "x-amz-rename-source-if-unmodified-since";
var _xart = "x-amz-request-token";
var _xasc = "x-amz-storage-class";
var _xasca = "x-amz-sdk-checksum-algorithm";
var _xasdv = "x-amz-skip-destination-validation";
var _xasebo = "x-amz-source-expected-bucket-owner";
var _xasse = "x-amz-server-side-encryption";
var _xasseakki = "x-amz-server-side-encryption-aws-kms-key-id";
var _xassebke = "x-amz-server-side-encryption-bucket-key-enabled";
var _xassec = "x-amz-server-side-encryption-context";
var _xasseca = "x-amz-server-side-encryption-customer-algorithm";
var _xasseck = "x-amz-server-side-encryption-customer-key";
var _xasseckM = "x-amz-server-side-encryption-customer-key-MD5";
var _xat = "x-amz-tagging";
var _xatc = "x-amz-tagging-count";
var _xatd = "x-amz-tagging-directive";
var _xatdmos = "x-amz-transition-default-minimum-object-size";
var _xavi = "x-amz-version-id";
var _xawob = "x-amz-write-offset-bytes";
var _xawrl = "x-amz-website-redirect-location";
var _xs = "xsi:type";
var n0 = "com.amazonaws.s3";
var _s_registry = TypeRegistry.for(_s);
var S3ServiceException$ = [-3, _s, "S3ServiceException", 0, [], []];
_s_registry.registerError(S3ServiceException$, S3ServiceException);
var n0_registry = TypeRegistry.for(n0);
var AccessDenied$ = [
  -3,
  n0,
  _AD,
  { [_e]: _c, [_hE]: 403 },
  [],
  []
];
n0_registry.registerError(AccessDenied$, AccessDenied);
var BucketAlreadyExists$ = [
  -3,
  n0,
  _BAE,
  { [_e]: _c, [_hE]: 409 },
  [],
  []
];
n0_registry.registerError(BucketAlreadyExists$, BucketAlreadyExists);
var BucketAlreadyOwnedByYou$ = [
  -3,
  n0,
  _BAOBY,
  { [_e]: _c, [_hE]: 409 },
  [],
  []
];
n0_registry.registerError(BucketAlreadyOwnedByYou$, BucketAlreadyOwnedByYou);
var EncryptionTypeMismatch$ = [
  -3,
  n0,
  _ETM,
  { [_e]: _c, [_hE]: 400 },
  [],
  []
];
n0_registry.registerError(EncryptionTypeMismatch$, EncryptionTypeMismatch);
var IdempotencyParameterMismatch$ = [
  -3,
  n0,
  _IPM,
  { [_e]: _c, [_hE]: 400 },
  [],
  []
];
n0_registry.registerError(IdempotencyParameterMismatch$, IdempotencyParameterMismatch);
var InvalidObjectState$ = [
  -3,
  n0,
  _IOS,
  { [_e]: _c, [_hE]: 403 },
  [_SC, _AT],
  [0, 0]
];
n0_registry.registerError(InvalidObjectState$, InvalidObjectState);
var InvalidRequest$ = [
  -3,
  n0,
  _IR,
  { [_e]: _c, [_hE]: 400 },
  [],
  []
];
n0_registry.registerError(InvalidRequest$, InvalidRequest);
var InvalidWriteOffset$ = [
  -3,
  n0,
  _IWO,
  { [_e]: _c, [_hE]: 400 },
  [],
  []
];
n0_registry.registerError(InvalidWriteOffset$, InvalidWriteOffset);
var NoSuchBucket$ = [
  -3,
  n0,
  _NSB,
  { [_e]: _c, [_hE]: 404 },
  [],
  []
];
n0_registry.registerError(NoSuchBucket$, NoSuchBucket);
var NoSuchKey$ = [
  -3,
  n0,
  _NSK,
  { [_e]: _c, [_hE]: 404 },
  [],
  []
];
n0_registry.registerError(NoSuchKey$, NoSuchKey);
var NoSuchUpload$ = [
  -3,
  n0,
  _NSU,
  { [_e]: _c, [_hE]: 404 },
  [],
  []
];
n0_registry.registerError(NoSuchUpload$, NoSuchUpload);
var NotFound$ = [
  -3,
  n0,
  _NF,
  { [_e]: _c },
  [],
  []
];
n0_registry.registerError(NotFound$, NotFound);
var ObjectAlreadyInActiveTierError$ = [
  -3,
  n0,
  _OAIATE,
  { [_e]: _c, [_hE]: 403 },
  [],
  []
];
n0_registry.registerError(ObjectAlreadyInActiveTierError$, ObjectAlreadyInActiveTierError);
var ObjectNotInActiveTierError$ = [
  -3,
  n0,
  _ONIATE,
  { [_e]: _c, [_hE]: 403 },
  [],
  []
];
n0_registry.registerError(ObjectNotInActiveTierError$, ObjectNotInActiveTierError);
var TooManyParts$ = [
  -3,
  n0,
  _TMP,
  { [_e]: _c, [_hE]: 400 },
  [],
  []
];
n0_registry.registerError(TooManyParts$, TooManyParts);
var errorTypeRegistries = [
  _s_registry,
  n0_registry
];
var CopySourceSSECustomerKey = [0, n0, _CSSSECK, 8, 0];
var NonEmptyKmsKeyArnString = [0, n0, _NEKKAS, 8, 0];
var SessionCredentialValue = [0, n0, _SCV, 8, 0];
var SSECustomerKey = [0, n0, _SSECK, 8, 0];
var SSEKMSEncryptionContext = [0, n0, _SSEKMSEC, 8, 0];
var SSEKMSKeyId = [0, n0, _SSEKMSKI, 8, 0];
var StreamingBlob = [0, n0, _SB, { [_st]: 1 }, 42];
var AbacStatus$ = [
  3,
  n0,
  _AS,
  0,
  [_S],
  [0]
];
var AbortIncompleteMultipartUpload$ = [
  3,
  n0,
  _AIMU,
  0,
  [_DAI],
  [1]
];
var AbortMultipartUploadOutput$ = [
  3,
  n0,
  _AMUO,
  0,
  [_RC],
  [[0, { [_hH]: _xarc }]]
];
var AbortMultipartUploadRequest$ = [
  3,
  n0,
  _AMUR,
  0,
  [_B, _K, _UI, _RP, _EBO, _IMIT],
  [[0, 1], [0, 1], [0, { [_hQ]: _uI }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [6, { [_hH]: _xaimit }]],
  3
];
var AccelerateConfiguration$ = [
  3,
  n0,
  _AC,
  0,
  [_S],
  [0]
];
var AccessControlPolicy$ = [
  3,
  n0,
  _ACP,
  0,
  [_G, _O],
  [[() => Grants, { [_xN]: _ACL }], () => Owner$]
];
var AccessControlTranslation$ = [
  3,
  n0,
  _ACT,
  0,
  [_O],
  [0],
  1
];
var AnalyticsAndOperator$ = [
  3,
  n0,
  _AAO,
  0,
  [_P, _T],
  [0, [() => TagSet, { [_xF]: 1, [_xN]: _Ta }]]
];
var AnalyticsConfiguration$ = [
  3,
  n0,
  _ACn,
  0,
  [_I, _SCA, _F],
  [0, () => StorageClassAnalysis$, [() => AnalyticsFilter$, 0]],
  2
];
var AnalyticsExportDestination$ = [
  3,
  n0,
  _AED,
  0,
  [_SBD],
  [() => AnalyticsS3BucketDestination$],
  1
];
var AnalyticsS3BucketDestination$ = [
  3,
  n0,
  _ASBD,
  0,
  [_Fo, _B, _BAI, _P],
  [0, 0, 0, 0],
  2
];
var BlockedEncryptionTypes$ = [
  3,
  n0,
  _BET,
  0,
  [_ET],
  [[() => EncryptionTypeList, { [_xF]: 1 }]]
];
var Bucket$ = [
  3,
  n0,
  _B,
  0,
  [_N, _CD, _BR, _BA],
  [0, 4, 0, 0]
];
var BucketInfo$ = [
  3,
  n0,
  _BI,
  0,
  [_DR, _Ty],
  [0, 0]
];
var BucketLifecycleConfiguration$ = [
  3,
  n0,
  _BLC,
  0,
  [_R],
  [[() => LifecycleRules, { [_xF]: 1, [_xN]: _Ru }]],
  1
];
var BucketLoggingStatus$ = [
  3,
  n0,
  _BLS,
  0,
  [_LE],
  [[() => LoggingEnabled$, 0]]
];
var Checksum$ = [
  3,
  n0,
  _C,
  0,
  [_CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _CT],
  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
];
var CommonPrefix$ = [
  3,
  n0,
  _CP,
  0,
  [_P],
  [0]
];
var CompletedMultipartUpload$ = [
  3,
  n0,
  _CMU,
  0,
  [_Pa],
  [[() => CompletedPartList, { [_xF]: 1, [_xN]: _Par }]]
];
var CompletedPart$ = [
  3,
  n0,
  _CPo,
  0,
  [_ETa, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _PN],
  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
];
var CompleteMultipartUploadOutput$ = [
  3,
  n0,
  _CMUO,
  { [_xN]: _CMUR },
  [_L, _B, _K, _E, _ETa, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _CT, _SSE, _VI, _SSEKMSKI, _BKE, _RC],
  [0, 0, 0, [0, { [_hH]: _xae }], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, [0, { [_hH]: _xasse }], [0, { [_hH]: _xavi }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarc }]]
];
var CompleteMultipartUploadRequest$ = [
  3,
  n0,
  _CMURo,
  0,
  [_B, _K, _UI, _MU, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _CT, _MOS, _RP, _EBO, _IM, _INM, _SSECA, _SSECK, _SSECKMD],
  [[0, 1], [0, 1], [0, { [_hQ]: _uI }], [() => CompletedMultipartUpload$, { [_hP]: 1, [_xN]: _CMUo }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [0, { [_hH]: _xact }], [1, { [_hH]: _xamos }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _IM_ }], [0, { [_hH]: _INM_ }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }]],
  3
];
var Condition$ = [
  3,
  n0,
  _Co,
  0,
  [_HECRE, _KPE],
  [0, 0]
];
var ContinuationEvent$ = [
  3,
  n0,
  _CE,
  0,
  [],
  []
];
var CopyObjectOutput$ = [
  3,
  n0,
  _COO,
  0,
  [_COR, _E, _CSVI, _VI, _SSE, _SSECA, _SSECKMD, _SSEKMSKI, _SSEKMSEC, _BKE, _RC],
  [[() => CopyObjectResult$, 16], [0, { [_hH]: _xae }], [0, { [_hH]: _xacsvi }], [0, { [_hH]: _xavi }], [0, { [_hH]: _xasse }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarc }]]
];
var CopyObjectRequest$ = [
  3,
  n0,
  _CORo,
  0,
  [_B, _CS, _K, _ACL_, _CC, _CA, _CDo, _CEo, _CL, _CTo, _CSIM, _CSIMS, _CSINM, _CSIUS, _Ex, _GFC, _GR, _GRACP, _GWACP, _IM, _INM, _M, _MD, _TD, _SSE, _SC, _WRL, _SSECA, _SSECK, _SSECKMD, _SSEKMSKI, _SSEKMSEC, _BKE, _CSSSECA, _CSSSECK, _CSSSECKMD, _RP, _Tag, _OLM, _OLRUD, _OLLHS, _EBO, _ESBO],
  [[0, 1], [0, { [_hH]: _xacs___ }], [0, 1], [0, { [_hH]: _xaa }], [0, { [_hH]: _CC_ }], [0, { [_hH]: _xaca }], [0, { [_hH]: _CD_ }], [0, { [_hH]: _CE_ }], [0, { [_hH]: _CL_ }], [0, { [_hH]: _CT_ }], [0, { [_hH]: _xacsim }], [4, { [_hH]: _xacsims }], [0, { [_hH]: _xacsinm }], [4, { [_hH]: _xacsius }], [4, { [_hH]: _Ex }], [0, { [_hH]: _xagfc }], [0, { [_hH]: _xagr }], [0, { [_hH]: _xagra }], [0, { [_hH]: _xagwa }], [0, { [_hH]: _IM_ }], [0, { [_hH]: _INM_ }], [128 | 0, { [_hPH]: _xam }], [0, { [_hH]: _xamd }], [0, { [_hH]: _xatd }], [0, { [_hH]: _xasse }], [0, { [_hH]: _xasc }], [0, { [_hH]: _xawrl }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xacssseca }], [() => CopySourceSSECustomerKey, { [_hH]: _xacssseck }], [0, { [_hH]: _xacssseckM }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xat }], [0, { [_hH]: _xaolm }], [5, { [_hH]: _xaolrud }], [0, { [_hH]: _xaollh }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xasebo }]],
  3
];
var CopyObjectResult$ = [
  3,
  n0,
  _COR,
  0,
  [_ETa, _LM, _CT, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe],
  [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
];
var CopyPartResult$ = [
  3,
  n0,
  _CPR,
  0,
  [_ETa, _LM, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe],
  [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
];
var CORSConfiguration$ = [
  3,
  n0,
  _CORSC,
  0,
  [_CORSR],
  [[() => CORSRules, { [_xF]: 1, [_xN]: _CORSRu }]],
  1
];
var CORSRule$ = [
  3,
  n0,
  _CORSRu,
  0,
  [_AM, _AO, _ID, _AH, _EH, _MAS],
  [[64 | 0, { [_xF]: 1, [_xN]: _AMl }], [64 | 0, { [_xF]: 1, [_xN]: _AOl }], 0, [64 | 0, { [_xF]: 1, [_xN]: _AHl }], [64 | 0, { [_xF]: 1, [_xN]: _EHx }], 1],
  2
];
var CreateBucketConfiguration$ = [
  3,
  n0,
  _CBC,
  0,
  [_LC, _L, _B, _T],
  [0, () => LocationInfo$, () => BucketInfo$, [() => TagSet, 0]]
];
var CreateBucketMetadataConfigurationRequest$ = [
  3,
  n0,
  _CBMCR,
  0,
  [_B, _MC, _CMDo, _CA, _EBO],
  [[0, 1], [() => MetadataConfiguration$, { [_hP]: 1, [_xN]: _MC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var CreateBucketMetadataTableConfigurationRequest$ = [
  3,
  n0,
  _CBMTCR,
  0,
  [_B, _MTC, _CMDo, _CA, _EBO],
  [[0, 1], [() => MetadataTableConfiguration$, { [_hP]: 1, [_xN]: _MTC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var CreateBucketOutput$ = [
  3,
  n0,
  _CBO,
  0,
  [_L, _BA],
  [[0, { [_hH]: _L }], [0, { [_hH]: _xaba }]]
];
var CreateBucketRequest$ = [
  3,
  n0,
  _CBR,
  0,
  [_B, _ACL_, _CBC, _GFC, _GR, _GRACP, _GW, _GWACP, _OLEFB, _OO, _BN],
  [[0, 1], [0, { [_hH]: _xaa }], [() => CreateBucketConfiguration$, { [_hP]: 1, [_xN]: _CBC }], [0, { [_hH]: _xagfc }], [0, { [_hH]: _xagr }], [0, { [_hH]: _xagra }], [0, { [_hH]: _xagw }], [0, { [_hH]: _xagwa }], [2, { [_hH]: _xabole }], [0, { [_hH]: _xaoo }], [0, { [_hH]: _xabn }]],
  1
];
var CreateMultipartUploadOutput$ = [
  3,
  n0,
  _CMUOr,
  { [_xN]: _IMUR },
  [_ADb, _ARI, _B, _K, _UI, _SSE, _SSECA, _SSECKMD, _SSEKMSKI, _SSEKMSEC, _BKE, _RC, _CA, _CT],
  [[4, { [_hH]: _xaad }], [0, { [_hH]: _xaari }], [0, { [_xN]: _B }], 0, 0, [0, { [_hH]: _xasse }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarc }], [0, { [_hH]: _xaca }], [0, { [_hH]: _xact }]]
];
var CreateMultipartUploadRequest$ = [
  3,
  n0,
  _CMURr,
  0,
  [_B, _K, _ACL_, _CC, _CDo, _CEo, _CL, _CTo, _Ex, _GFC, _GR, _GRACP, _GWACP, _M, _SSE, _SC, _WRL, _SSECA, _SSECK, _SSECKMD, _SSEKMSKI, _SSEKMSEC, _BKE, _RP, _Tag, _OLM, _OLRUD, _OLLHS, _EBO, _CA, _CT],
  [[0, 1], [0, 1], [0, { [_hH]: _xaa }], [0, { [_hH]: _CC_ }], [0, { [_hH]: _CD_ }], [0, { [_hH]: _CE_ }], [0, { [_hH]: _CL_ }], [0, { [_hH]: _CT_ }], [4, { [_hH]: _Ex }], [0, { [_hH]: _xagfc }], [0, { [_hH]: _xagr }], [0, { [_hH]: _xagra }], [0, { [_hH]: _xagwa }], [128 | 0, { [_hPH]: _xam }], [0, { [_hH]: _xasse }], [0, { [_hH]: _xasc }], [0, { [_hH]: _xawrl }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xat }], [0, { [_hH]: _xaolm }], [5, { [_hH]: _xaolrud }], [0, { [_hH]: _xaollh }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xaca }], [0, { [_hH]: _xact }]],
  2
];
var CreateSessionOutput$ = [
  3,
  n0,
  _CSO,
  { [_xN]: _CSR },
  [_Cr, _SSE, _SSEKMSKI, _SSEKMSEC, _BKE],
  [[() => SessionCredentials$, { [_xN]: _Cr }], [0, { [_hH]: _xasse }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }]],
  1
];
var CreateSessionRequest$ = [
  3,
  n0,
  _CSRr,
  0,
  [_B, _SM, _SSE, _SSEKMSKI, _SSEKMSEC, _BKE],
  [[0, 1], [0, { [_hH]: _xacsm }], [0, { [_hH]: _xasse }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }]],
  1
];
var CSVInput$ = [
  3,
  n0,
  _CSVIn,
  0,
  [_FHI, _Com, _QEC, _RD, _FD, _QC, _AQRD],
  [0, 0, 0, 0, 0, 0, 2]
];
var CSVOutput$ = [
  3,
  n0,
  _CSVO,
  0,
  [_QF, _QEC, _RD, _FD, _QC],
  [0, 0, 0, 0, 0]
];
var DefaultRetention$ = [
  3,
  n0,
  _DRe,
  0,
  [_Mo, _D, _Y],
  [0, 1, 1]
];
var Delete$ = [
  3,
  n0,
  _De,
  0,
  [_Ob, _Q],
  [[() => ObjectIdentifierList, { [_xF]: 1, [_xN]: _Obj }], 2],
  1
];
var DeleteBucketAnalyticsConfigurationRequest$ = [
  3,
  n0,
  _DBACR,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var DeleteBucketCorsRequest$ = [
  3,
  n0,
  _DBCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketEncryptionRequest$ = [
  3,
  n0,
  _DBER,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketIntelligentTieringConfigurationRequest$ = [
  3,
  n0,
  _DBITCR,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var DeleteBucketInventoryConfigurationRequest$ = [
  3,
  n0,
  _DBICR,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var DeleteBucketLifecycleRequest$ = [
  3,
  n0,
  _DBLR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketMetadataConfigurationRequest$ = [
  3,
  n0,
  _DBMCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketMetadataTableConfigurationRequest$ = [
  3,
  n0,
  _DBMTCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketMetricsConfigurationRequest$ = [
  3,
  n0,
  _DBMCRe,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var DeleteBucketOwnershipControlsRequest$ = [
  3,
  n0,
  _DBOCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketPolicyRequest$ = [
  3,
  n0,
  _DBPR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketReplicationRequest$ = [
  3,
  n0,
  _DBRR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketRequest$ = [
  3,
  n0,
  _DBR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketTaggingRequest$ = [
  3,
  n0,
  _DBTR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeleteBucketWebsiteRequest$ = [
  3,
  n0,
  _DBWR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var DeletedObject$ = [
  3,
  n0,
  _DO,
  0,
  [_K, _VI, _DM, _DMVI],
  [0, 0, 2, 0]
];
var DeleteMarkerEntry$ = [
  3,
  n0,
  _DME,
  0,
  [_O, _K, _VI, _IL, _LM],
  [() => Owner$, 0, 0, 2, 4]
];
var DeleteMarkerReplication$ = [
  3,
  n0,
  _DMR,
  0,
  [_S],
  [0]
];
var DeleteObjectOutput$ = [
  3,
  n0,
  _DOO,
  0,
  [_DM, _VI, _RC],
  [[2, { [_hH]: _xadm }], [0, { [_hH]: _xavi }], [0, { [_hH]: _xarc }]]
];
var DeleteObjectRequest$ = [
  3,
  n0,
  _DOR,
  0,
  [_B, _K, _MFA, _VI, _RP, _BGR, _EBO, _IM, _IMLMT, _IMS],
  [[0, 1], [0, 1], [0, { [_hH]: _xam_ }], [0, { [_hQ]: _vI }], [0, { [_hH]: _xarp }], [2, { [_hH]: _xabgr }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _IM_ }], [6, { [_hH]: _xaimlmt }], [1, { [_hH]: _xaims }]],
  2
];
var DeleteObjectsOutput$ = [
  3,
  n0,
  _DOOe,
  { [_xN]: _DRel },
  [_Del, _RC, _Er],
  [[() => DeletedObjects, { [_xF]: 1 }], [0, { [_hH]: _xarc }], [() => Errors, { [_xF]: 1, [_xN]: _Err }]]
];
var DeleteObjectsRequest$ = [
  3,
  n0,
  _DORe,
  0,
  [_B, _De, _MFA, _RP, _BGR, _EBO, _CA],
  [[0, 1], [() => Delete$, { [_hP]: 1, [_xN]: _De }], [0, { [_hH]: _xam_ }], [0, { [_hH]: _xarp }], [2, { [_hH]: _xabgr }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xasca }]],
  2
];
var DeleteObjectTaggingOutput$ = [
  3,
  n0,
  _DOTO,
  0,
  [_VI],
  [[0, { [_hH]: _xavi }]]
];
var DeleteObjectTaggingRequest$ = [
  3,
  n0,
  _DOTR,
  0,
  [_B, _K, _VI, _EBO],
  [[0, 1], [0, 1], [0, { [_hQ]: _vI }], [0, { [_hH]: _xaebo }]],
  2
];
var DeletePublicAccessBlockRequest$ = [
  3,
  n0,
  _DPABR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var Destination$ = [
  3,
  n0,
  _Des,
  0,
  [_B, _A, _SC, _ACT, _EC, _RT, _Me],
  [0, 0, 0, () => AccessControlTranslation$, () => EncryptionConfiguration$, () => ReplicationTime$, () => Metrics$],
  1
];
var DestinationResult$ = [
  3,
  n0,
  _DRes,
  0,
  [_TBT, _TBA, _TN],
  [0, 0, 0]
];
var Encryption$ = [
  3,
  n0,
  _En,
  0,
  [_ET, _KMSKI, _KMSC],
  [0, [() => SSEKMSKeyId, 0], 0],
  1
];
var EncryptionConfiguration$ = [
  3,
  n0,
  _EC,
  0,
  [_RKKID],
  [0]
];
var EndEvent$ = [
  3,
  n0,
  _EE,
  0,
  [],
  []
];
var _Error$ = [
  3,
  n0,
  _Err,
  0,
  [_K, _VI, _Cod, _Mes],
  [0, 0, 0, 0]
];
var ErrorDetails$ = [
  3,
  n0,
  _ED,
  0,
  [_ECr, _EM],
  [0, 0]
];
var ErrorDocument$ = [
  3,
  n0,
  _EDr,
  0,
  [_K],
  [0],
  1
];
var EventBridgeConfiguration$ = [
  3,
  n0,
  _EBC,
  0,
  [],
  []
];
var ExistingObjectReplication$ = [
  3,
  n0,
  _EOR,
  0,
  [_S],
  [0],
  1
];
var FilterRule$ = [
  3,
  n0,
  _FR,
  0,
  [_N, _V],
  [0, 0]
];
var GetBucketAbacOutput$ = [
  3,
  n0,
  _GBAO,
  0,
  [_AS],
  [[() => AbacStatus$, 16]]
];
var GetBucketAbacRequest$ = [
  3,
  n0,
  _GBAR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketAccelerateConfigurationOutput$ = [
  3,
  n0,
  _GBACO,
  { [_xN]: _AC },
  [_S, _RC],
  [0, [0, { [_hH]: _xarc }]]
];
var GetBucketAccelerateConfigurationRequest$ = [
  3,
  n0,
  _GBACR,
  0,
  [_B, _EBO, _RP],
  [[0, 1], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xarp }]],
  1
];
var GetBucketAclOutput$ = [
  3,
  n0,
  _GBAOe,
  { [_xN]: _ACP },
  [_O, _G],
  [() => Owner$, [() => Grants, { [_xN]: _ACL }]]
];
var GetBucketAclRequest$ = [
  3,
  n0,
  _GBARe,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketAnalyticsConfigurationOutput$ = [
  3,
  n0,
  _GBACOe,
  0,
  [_ACn],
  [[() => AnalyticsConfiguration$, 16]]
];
var GetBucketAnalyticsConfigurationRequest$ = [
  3,
  n0,
  _GBACRe,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var GetBucketCorsOutput$ = [
  3,
  n0,
  _GBCO,
  { [_xN]: _CORSC },
  [_CORSR],
  [[() => CORSRules, { [_xF]: 1, [_xN]: _CORSRu }]]
];
var GetBucketCorsRequest$ = [
  3,
  n0,
  _GBCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketEncryptionOutput$ = [
  3,
  n0,
  _GBEO,
  0,
  [_SSEC],
  [[() => ServerSideEncryptionConfiguration$, 16]]
];
var GetBucketEncryptionRequest$ = [
  3,
  n0,
  _GBER,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketIntelligentTieringConfigurationOutput$ = [
  3,
  n0,
  _GBITCO,
  0,
  [_ITC],
  [[() => IntelligentTieringConfiguration$, 16]]
];
var GetBucketIntelligentTieringConfigurationRequest$ = [
  3,
  n0,
  _GBITCR,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var GetBucketInventoryConfigurationOutput$ = [
  3,
  n0,
  _GBICO,
  0,
  [_IC],
  [[() => InventoryConfiguration$, 16]]
];
var GetBucketInventoryConfigurationRequest$ = [
  3,
  n0,
  _GBICR,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var GetBucketLifecycleConfigurationOutput$ = [
  3,
  n0,
  _GBLCO,
  { [_xN]: _LCi },
  [_R, _TDMOS],
  [[() => LifecycleRules, { [_xF]: 1, [_xN]: _Ru }], [0, { [_hH]: _xatdmos }]]
];
var GetBucketLifecycleConfigurationRequest$ = [
  3,
  n0,
  _GBLCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketLocationOutput$ = [
  3,
  n0,
  _GBLO,
  { [_xN]: _LC },
  [_LC],
  [0]
];
var GetBucketLocationRequest$ = [
  3,
  n0,
  _GBLR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketLoggingOutput$ = [
  3,
  n0,
  _GBLOe,
  { [_xN]: _BLS },
  [_LE],
  [[() => LoggingEnabled$, 0]]
];
var GetBucketLoggingRequest$ = [
  3,
  n0,
  _GBLRe,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketMetadataConfigurationOutput$ = [
  3,
  n0,
  _GBMCO,
  0,
  [_GBMCR],
  [[() => GetBucketMetadataConfigurationResult$, 16]]
];
var GetBucketMetadataConfigurationRequest$ = [
  3,
  n0,
  _GBMCRe,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketMetadataConfigurationResult$ = [
  3,
  n0,
  _GBMCR,
  0,
  [_MCR],
  [() => MetadataConfigurationResult$],
  1
];
var GetBucketMetadataTableConfigurationOutput$ = [
  3,
  n0,
  _GBMTCO,
  0,
  [_GBMTCR],
  [[() => GetBucketMetadataTableConfigurationResult$, 16]]
];
var GetBucketMetadataTableConfigurationRequest$ = [
  3,
  n0,
  _GBMTCRe,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketMetadataTableConfigurationResult$ = [
  3,
  n0,
  _GBMTCR,
  0,
  [_MTCR, _S, _Err],
  [() => MetadataTableConfigurationResult$, 0, () => ErrorDetails$],
  2
];
var GetBucketMetricsConfigurationOutput$ = [
  3,
  n0,
  _GBMCOe,
  0,
  [_MCe],
  [[() => MetricsConfiguration$, 16]]
];
var GetBucketMetricsConfigurationRequest$ = [
  3,
  n0,
  _GBMCRet,
  0,
  [_B, _I, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [0, { [_hH]: _xaebo }]],
  2
];
var GetBucketNotificationConfigurationRequest$ = [
  3,
  n0,
  _GBNCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketOwnershipControlsOutput$ = [
  3,
  n0,
  _GBOCO,
  0,
  [_OC],
  [[() => OwnershipControls$, 16]]
];
var GetBucketOwnershipControlsRequest$ = [
  3,
  n0,
  _GBOCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketPolicyOutput$ = [
  3,
  n0,
  _GBPO,
  0,
  [_Po],
  [[0, 16]]
];
var GetBucketPolicyRequest$ = [
  3,
  n0,
  _GBPR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketPolicyStatusOutput$ = [
  3,
  n0,
  _GBPSO,
  0,
  [_PS],
  [[() => PolicyStatus$, 16]]
];
var GetBucketPolicyStatusRequest$ = [
  3,
  n0,
  _GBPSR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketReplicationOutput$ = [
  3,
  n0,
  _GBRO,
  0,
  [_RCe],
  [[() => ReplicationConfiguration$, 16]]
];
var GetBucketReplicationRequest$ = [
  3,
  n0,
  _GBRR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketRequestPaymentOutput$ = [
  3,
  n0,
  _GBRPO,
  { [_xN]: _RPC },
  [_Pay],
  [0]
];
var GetBucketRequestPaymentRequest$ = [
  3,
  n0,
  _GBRPR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketTaggingOutput$ = [
  3,
  n0,
  _GBTO,
  { [_xN]: _Tag },
  [_TS],
  [[() => TagSet, 0]],
  1
];
var GetBucketTaggingRequest$ = [
  3,
  n0,
  _GBTR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketVersioningOutput$ = [
  3,
  n0,
  _GBVO,
  { [_xN]: _VC },
  [_S, _MFAD],
  [0, [0, { [_xN]: _MDf }]]
];
var GetBucketVersioningRequest$ = [
  3,
  n0,
  _GBVR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetBucketWebsiteOutput$ = [
  3,
  n0,
  _GBWO,
  { [_xN]: _WC },
  [_RART, _IDn, _EDr, _RR],
  [() => RedirectAllRequestsTo$, () => IndexDocument$, () => ErrorDocument$, [() => RoutingRules, 0]]
];
var GetBucketWebsiteRequest$ = [
  3,
  n0,
  _GBWR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetObjectAclOutput$ = [
  3,
  n0,
  _GOAO,
  { [_xN]: _ACP },
  [_O, _G, _RC],
  [() => Owner$, [() => Grants, { [_xN]: _ACL }], [0, { [_hH]: _xarc }]]
];
var GetObjectAclRequest$ = [
  3,
  n0,
  _GOAR,
  0,
  [_B, _K, _VI, _RP, _EBO],
  [[0, 1], [0, 1], [0, { [_hQ]: _vI }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }]],
  2
];
var GetObjectAttributesOutput$ = [
  3,
  n0,
  _GOAOe,
  { [_xN]: _GOARe },
  [_DM, _LM, _VI, _RC, _ETa, _C, _OP, _SC, _OS],
  [[2, { [_hH]: _xadm }], [4, { [_hH]: _LM_ }], [0, { [_hH]: _xavi }], [0, { [_hH]: _xarc }], 0, () => Checksum$, [() => GetObjectAttributesParts$, 0], 0, 1]
];
var GetObjectAttributesParts$ = [
  3,
  n0,
  _GOAP,
  0,
  [_TPC, _PNM, _NPNM, _MP, _IT, _Pa],
  [[1, { [_xN]: _PC }], 0, 0, 1, 2, [() => PartsList, { [_xF]: 1, [_xN]: _Par }]]
];
var GetObjectAttributesRequest$ = [
  3,
  n0,
  _GOARet,
  0,
  [_B, _K, _OA, _VI, _MP, _PNM, _SSECA, _SSECK, _SSECKMD, _RP, _EBO],
  [[0, 1], [0, 1], [64 | 0, { [_hH]: _xaoa }], [0, { [_hQ]: _vI }], [1, { [_hH]: _xamp }], [0, { [_hH]: _xapnm }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }]],
  3
];
var GetObjectLegalHoldOutput$ = [
  3,
  n0,
  _GOLHO,
  0,
  [_LH],
  [[() => ObjectLockLegalHold$, { [_hP]: 1, [_xN]: _LH }]]
];
var GetObjectLegalHoldRequest$ = [
  3,
  n0,
  _GOLHR,
  0,
  [_B, _K, _VI, _RP, _EBO],
  [[0, 1], [0, 1], [0, { [_hQ]: _vI }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }]],
  2
];
var GetObjectLockConfigurationOutput$ = [
  3,
  n0,
  _GOLCO,
  0,
  [_OLC],
  [[() => ObjectLockConfiguration$, 16]]
];
var GetObjectLockConfigurationRequest$ = [
  3,
  n0,
  _GOLCR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GetObjectOutput$ = [
  3,
  n0,
  _GOO,
  0,
  [_Bo, _DM, _AR, _E, _Re, _LM, _CLo, _ETa, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _CT, _MM, _VI, _CC, _CDo, _CEo, _CL, _CR, _CTo, _Ex, _ES, _WRL, _SSE, _M, _SSECA, _SSECKMD, _SSEKMSKI, _BKE, _SC, _RC, _RS, _PC, _TC, _OLM, _OLRUD, _OLLHS],
  [[() => StreamingBlob, 16], [2, { [_hH]: _xadm }], [0, { [_hH]: _ar }], [0, { [_hH]: _xae }], [0, { [_hH]: _xar }], [4, { [_hH]: _LM_ }], [1, { [_hH]: _CL__ }], [0, { [_hH]: _ETa }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [0, { [_hH]: _xact }], [1, { [_hH]: _xamm }], [0, { [_hH]: _xavi }], [0, { [_hH]: _CC_ }], [0, { [_hH]: _CD_ }], [0, { [_hH]: _CE_ }], [0, { [_hH]: _CL_ }], [0, { [_hH]: _CR_ }], [0, { [_hH]: _CT_ }], [4, { [_hH]: _Ex }], [0, { [_hH]: _ES }], [0, { [_hH]: _xawrl }], [0, { [_hH]: _xasse }], [128 | 0, { [_hPH]: _xam }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xasc }], [0, { [_hH]: _xarc }], [0, { [_hH]: _xars }], [1, { [_hH]: _xampc }], [1, { [_hH]: _xatc }], [0, { [_hH]: _xaolm }], [5, { [_hH]: _xaolrud }], [0, { [_hH]: _xaollh }]]
];
var GetObjectRequest$ = [
  3,
  n0,
  _GOR,
  0,
  [_B, _K, _IM, _IMSf, _INM, _IUS, _Ra, _RCC, _RCD, _RCE, _RCL, _RCT, _RE, _VI, _SSECA, _SSECK, _SSECKMD, _RP, _PN, _EBO, _CMh],
  [[0, 1], [0, 1], [0, { [_hH]: _IM_ }], [4, { [_hH]: _IMS_ }], [0, { [_hH]: _INM_ }], [4, { [_hH]: _IUS_ }], [0, { [_hH]: _Ra }], [0, { [_hQ]: _rcc }], [0, { [_hQ]: _rcd }], [0, { [_hQ]: _rce }], [0, { [_hQ]: _rcl }], [0, { [_hQ]: _rct }], [6, { [_hQ]: _re }], [0, { [_hQ]: _vI }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [0, { [_hH]: _xarp }], [1, { [_hQ]: _pN }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xacm_ }]],
  2
];
var GetObjectRetentionOutput$ = [
  3,
  n0,
  _GORO,
  0,
  [_Ret],
  [[() => ObjectLockRetention$, { [_hP]: 1, [_xN]: _Ret }]]
];
var GetObjectRetentionRequest$ = [
  3,
  n0,
  _GORR,
  0,
  [_B, _K, _VI, _RP, _EBO],
  [[0, 1], [0, 1], [0, { [_hQ]: _vI }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }]],
  2
];
var GetObjectTaggingOutput$ = [
  3,
  n0,
  _GOTO,
  { [_xN]: _Tag },
  [_TS, _VI],
  [[() => TagSet, 0], [0, { [_hH]: _xavi }]],
  1
];
var GetObjectTaggingRequest$ = [
  3,
  n0,
  _GOTR,
  0,
  [_B, _K, _VI, _EBO, _RP],
  [[0, 1], [0, 1], [0, { [_hQ]: _vI }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xarp }]],
  2
];
var GetObjectTorrentOutput$ = [
  3,
  n0,
  _GOTOe,
  0,
  [_Bo, _RC],
  [[() => StreamingBlob, 16], [0, { [_hH]: _xarc }]]
];
var GetObjectTorrentRequest$ = [
  3,
  n0,
  _GOTRe,
  0,
  [_B, _K, _RP, _EBO],
  [[0, 1], [0, 1], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }]],
  2
];
var GetPublicAccessBlockOutput$ = [
  3,
  n0,
  _GPABO,
  0,
  [_PABC],
  [[() => PublicAccessBlockConfiguration$, 16]]
];
var GetPublicAccessBlockRequest$ = [
  3,
  n0,
  _GPABR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var GlacierJobParameters$ = [
  3,
  n0,
  _GJP,
  0,
  [_Ti],
  [0],
  1
];
var Grant$ = [
  3,
  n0,
  _Gr,
  0,
  [_Gra, _Pe],
  [[() => Grantee$, { [_xNm]: [_x, _hi] }], 0]
];
var Grantee$ = [
  3,
  n0,
  _Gra,
  0,
  [_Ty, _DN, _EA, _ID, _URI],
  [[0, { [_xA]: 1, [_xN]: _xs }], 0, 0, 0, 0],
  1
];
var HeadBucketOutput$ = [
  3,
  n0,
  _HBO,
  0,
  [_BA, _BLT, _BLN, _BR, _APA],
  [[0, { [_hH]: _xaba }], [0, { [_hH]: _xablt }], [0, { [_hH]: _xabln }], [0, { [_hH]: _xabr }], [2, { [_hH]: _xaapa }]]
];
var HeadBucketRequest$ = [
  3,
  n0,
  _HBR,
  0,
  [_B, _EBO],
  [[0, 1], [0, { [_hH]: _xaebo }]],
  1
];
var HeadObjectOutput$ = [
  3,
  n0,
  _HOO,
  0,
  [_DM, _AR, _E, _Re, _ASr, _LM, _CLo, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _CT, _ETa, _MM, _VI, _CC, _CDo, _CEo, _CL, _CTo, _CR, _Ex, _ES, _WRL, _SSE, _M, _SSECA, _SSECKMD, _SSEKMSKI, _BKE, _SC, _RC, _RS, _PC, _TC, _OLM, _OLRUD, _OLLHS],
  [[2, { [_hH]: _xadm }], [0, { [_hH]: _ar }], [0, { [_hH]: _xae }], [0, { [_hH]: _xar }], [0, { [_hH]: _xaas }], [4, { [_hH]: _LM_ }], [1, { [_hH]: _CL__ }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [0, { [_hH]: _xact }], [0, { [_hH]: _ETa }], [1, { [_hH]: _xamm }], [0, { [_hH]: _xavi }], [0, { [_hH]: _CC_ }], [0, { [_hH]: _CD_ }], [0, { [_hH]: _CE_ }], [0, { [_hH]: _CL_ }], [0, { [_hH]: _CT_ }], [0, { [_hH]: _CR_ }], [4, { [_hH]: _Ex }], [0, { [_hH]: _ES }], [0, { [_hH]: _xawrl }], [0, { [_hH]: _xasse }], [128 | 0, { [_hPH]: _xam }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xasc }], [0, { [_hH]: _xarc }], [0, { [_hH]: _xars }], [1, { [_hH]: _xampc }], [1, { [_hH]: _xatc }], [0, { [_hH]: _xaolm }], [5, { [_hH]: _xaolrud }], [0, { [_hH]: _xaollh }]]
];
var HeadObjectRequest$ = [
  3,
  n0,
  _HOR,
  0,
  [_B, _K, _IM, _IMSf, _INM, _IUS, _Ra, _RCC, _RCD, _RCE, _RCL, _RCT, _RE, _VI, _SSECA, _SSECK, _SSECKMD, _RP, _PN, _EBO, _CMh],
  [[0, 1], [0, 1], [0, { [_hH]: _IM_ }], [4, { [_hH]: _IMS_ }], [0, { [_hH]: _INM_ }], [4, { [_hH]: _IUS_ }], [0, { [_hH]: _Ra }], [0, { [_hQ]: _rcc }], [0, { [_hQ]: _rcd }], [0, { [_hQ]: _rce }], [0, { [_hQ]: _rcl }], [0, { [_hQ]: _rct }], [6, { [_hQ]: _re }], [0, { [_hQ]: _vI }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [0, { [_hH]: _xarp }], [1, { [_hQ]: _pN }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xacm_ }]],
  2
];
var IndexDocument$ = [
  3,
  n0,
  _IDn,
  0,
  [_Su],
  [0],
  1
];
var Initiator$ = [
  3,
  n0,
  _In,
  0,
  [_ID, _DN],
  [0, 0]
];
var InputSerialization$ = [
  3,
  n0,
  _IS,
  0,
  [_CSV, _CTom, _JSON, _Parq],
  [() => CSVInput$, 0, () => JSONInput$, () => ParquetInput$]
];
var IntelligentTieringAndOperator$ = [
  3,
  n0,
  _ITAO,
  0,
  [_P, _T],
  [0, [() => TagSet, { [_xF]: 1, [_xN]: _Ta }]]
];
var IntelligentTieringConfiguration$ = [
  3,
  n0,
  _ITC,
  0,
  [_I, _S, _Tie, _F],
  [0, 0, [() => TieringList, { [_xF]: 1, [_xN]: _Tier }], [() => IntelligentTieringFilter$, 0]],
  3
];
var IntelligentTieringFilter$ = [
  3,
  n0,
  _ITF,
  0,
  [_P, _Ta, _An],
  [0, () => Tag$, [() => IntelligentTieringAndOperator$, 0]]
];
var InventoryConfiguration$ = [
  3,
  n0,
  _IC,
  0,
  [_Des, _IE, _I, _IOV, _Sc, _F, _OF],
  [[() => InventoryDestination$, 0], 2, 0, 0, () => InventorySchedule$, () => InventoryFilter$, [() => InventoryOptionalFields, 0]],
  5
];
var InventoryDestination$ = [
  3,
  n0,
  _IDnv,
  0,
  [_SBD],
  [[() => InventoryS3BucketDestination$, 0]],
  1
];
var InventoryEncryption$ = [
  3,
  n0,
  _IEn,
  0,
  [_SSES, _SSEKMS],
  [[() => SSES3$, { [_xN]: _SS }], [() => SSEKMS$, { [_xN]: _SK }]]
];
var InventoryFilter$ = [
  3,
  n0,
  _IF,
  0,
  [_P],
  [0],
  1
];
var InventoryS3BucketDestination$ = [
  3,
  n0,
  _ISBD,
  0,
  [_B, _Fo, _AI, _P, _En],
  [0, 0, 0, 0, [() => InventoryEncryption$, 0]],
  2
];
var InventorySchedule$ = [
  3,
  n0,
  _ISn,
  0,
  [_Fr],
  [0],
  1
];
var InventoryTableConfiguration$ = [
  3,
  n0,
  _ITCn,
  0,
  [_CSo, _EC],
  [0, () => MetadataTableEncryptionConfiguration$],
  1
];
var InventoryTableConfigurationResult$ = [
  3,
  n0,
  _ITCR,
  0,
  [_CSo, _TSa, _Err, _TNa, _TA],
  [0, 0, () => ErrorDetails$, 0, 0],
  1
];
var InventoryTableConfigurationUpdates$ = [
  3,
  n0,
  _ITCU,
  0,
  [_CSo, _EC],
  [0, () => MetadataTableEncryptionConfiguration$],
  1
];
var JournalTableConfiguration$ = [
  3,
  n0,
  _JTC,
  0,
  [_REe, _EC],
  [() => RecordExpiration$, () => MetadataTableEncryptionConfiguration$],
  1
];
var JournalTableConfigurationResult$ = [
  3,
  n0,
  _JTCR,
  0,
  [_TSa, _TNa, _REe, _Err, _TA],
  [0, 0, () => RecordExpiration$, () => ErrorDetails$, 0],
  3
];
var JournalTableConfigurationUpdates$ = [
  3,
  n0,
  _JTCU,
  0,
  [_REe],
  [() => RecordExpiration$],
  1
];
var JSONInput$ = [
  3,
  n0,
  _JSONI,
  0,
  [_Ty],
  [0]
];
var JSONOutput$ = [
  3,
  n0,
  _JSONO,
  0,
  [_RD],
  [0]
];
var LambdaFunctionConfiguration$ = [
  3,
  n0,
  _LFC,
  0,
  [_LFA, _Ev, _I, _F],
  [[0, { [_xN]: _CF }], [64 | 0, { [_xF]: 1, [_xN]: _Eve }], 0, [() => NotificationConfigurationFilter$, 0]],
  2
];
var LifecycleExpiration$ = [
  3,
  n0,
  _LEi,
  0,
  [_Da, _D, _EODM],
  [5, 1, 2]
];
var LifecycleRule$ = [
  3,
  n0,
  _LR,
  0,
  [_S, _E, _ID, _P, _F, _Tr, _NVT, _NVE, _AIMU],
  [0, () => LifecycleExpiration$, 0, 0, [() => LifecycleRuleFilter$, 0], [() => TransitionList, { [_xF]: 1, [_xN]: _Tra }], [() => NoncurrentVersionTransitionList, { [_xF]: 1, [_xN]: _NVTo }], () => NoncurrentVersionExpiration$, () => AbortIncompleteMultipartUpload$],
  1
];
var LifecycleRuleAndOperator$ = [
  3,
  n0,
  _LRAO,
  0,
  [_P, _T, _OSGT, _OSLT],
  [0, [() => TagSet, { [_xF]: 1, [_xN]: _Ta }], 1, 1]
];
var LifecycleRuleFilter$ = [
  3,
  n0,
  _LRF,
  0,
  [_P, _Ta, _OSGT, _OSLT, _An],
  [0, () => Tag$, 1, 1, [() => LifecycleRuleAndOperator$, 0]]
];
var ListBucketAnalyticsConfigurationsOutput$ = [
  3,
  n0,
  _LBACO,
  { [_xN]: _LBACR },
  [_IT, _CTon, _NCT, _ACLn],
  [2, 0, 0, [() => AnalyticsConfigurationList, { [_xF]: 1, [_xN]: _ACn }]]
];
var ListBucketAnalyticsConfigurationsRequest$ = [
  3,
  n0,
  _LBACRi,
  0,
  [_B, _CTon, _EBO],
  [[0, 1], [0, { [_hQ]: _ct }], [0, { [_hH]: _xaebo }]],
  1
];
var ListBucketIntelligentTieringConfigurationsOutput$ = [
  3,
  n0,
  _LBITCO,
  0,
  [_IT, _CTon, _NCT, _ITCL],
  [2, 0, 0, [() => IntelligentTieringConfigurationList, { [_xF]: 1, [_xN]: _ITC }]]
];
var ListBucketIntelligentTieringConfigurationsRequest$ = [
  3,
  n0,
  _LBITCR,
  0,
  [_B, _CTon, _EBO],
  [[0, 1], [0, { [_hQ]: _ct }], [0, { [_hH]: _xaebo }]],
  1
];
var ListBucketInventoryConfigurationsOutput$ = [
  3,
  n0,
  _LBICO,
  { [_xN]: _LICR },
  [_CTon, _ICL, _IT, _NCT],
  [0, [() => InventoryConfigurationList, { [_xF]: 1, [_xN]: _IC }], 2, 0]
];
var ListBucketInventoryConfigurationsRequest$ = [
  3,
  n0,
  _LBICR,
  0,
  [_B, _CTon, _EBO],
  [[0, 1], [0, { [_hQ]: _ct }], [0, { [_hH]: _xaebo }]],
  1
];
var ListBucketMetricsConfigurationsOutput$ = [
  3,
  n0,
  _LBMCO,
  { [_xN]: _LMCR },
  [_IT, _CTon, _NCT, _MCL],
  [2, 0, 0, [() => MetricsConfigurationList, { [_xF]: 1, [_xN]: _MCe }]]
];
var ListBucketMetricsConfigurationsRequest$ = [
  3,
  n0,
  _LBMCR,
  0,
  [_B, _CTon, _EBO],
  [[0, 1], [0, { [_hQ]: _ct }], [0, { [_hH]: _xaebo }]],
  1
];
var ListBucketsOutput$ = [
  3,
  n0,
  _LBO,
  { [_xN]: _LAMBR },
  [_Bu, _O, _CTon, _P],
  [[() => Buckets, 0], () => Owner$, 0, 0]
];
var ListBucketsRequest$ = [
  3,
  n0,
  _LBR,
  0,
  [_MB, _CTon, _P, _BR],
  [[1, { [_hQ]: _mb }], [0, { [_hQ]: _ct }], [0, { [_hQ]: _p }], [0, { [_hQ]: _br }]]
];
var ListDirectoryBucketsOutput$ = [
  3,
  n0,
  _LDBO,
  { [_xN]: _LAMDBR },
  [_Bu, _CTon],
  [[() => Buckets, 0], 0]
];
var ListDirectoryBucketsRequest$ = [
  3,
  n0,
  _LDBR,
  0,
  [_CTon, _MDB],
  [[0, { [_hQ]: _ct }], [1, { [_hQ]: _mdb }]]
];
var ListMultipartUploadsOutput$ = [
  3,
  n0,
  _LMUO,
  { [_xN]: _LMUR },
  [_B, _KM, _UIM, _NKM, _P, _Deli, _NUIM, _MUa, _IT, _U, _CPom, _ETn, _RC],
  [0, 0, 0, 0, 0, 0, 0, 1, 2, [() => MultipartUploadList, { [_xF]: 1, [_xN]: _Up }], [() => CommonPrefixList, { [_xF]: 1 }], 0, [0, { [_hH]: _xarc }]]
];
var ListMultipartUploadsRequest$ = [
  3,
  n0,
  _LMURi,
  0,
  [_B, _Deli, _ETn, _KM, _MUa, _P, _UIM, _EBO, _RP],
  [[0, 1], [0, { [_hQ]: _d }], [0, { [_hQ]: _et }], [0, { [_hQ]: _km }], [1, { [_hQ]: _mu }], [0, { [_hQ]: _p }], [0, { [_hQ]: _uim }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xarp }]],
  1
];
var ListObjectsOutput$ = [
  3,
  n0,
  _LOO,
  { [_xN]: _LBRi },
  [_IT, _Ma, _NM, _Con, _N, _P, _Deli, _MK, _CPom, _ETn, _RC],
  [2, 0, 0, [() => ObjectList, { [_xF]: 1 }], 0, 0, 0, 1, [() => CommonPrefixList, { [_xF]: 1 }], 0, [0, { [_hH]: _xarc }]]
];
var ListObjectsRequest$ = [
  3,
  n0,
  _LOR,
  0,
  [_B, _Deli, _ETn, _Ma, _MK, _P, _RP, _EBO, _OOA],
  [[0, 1], [0, { [_hQ]: _d }], [0, { [_hQ]: _et }], [0, { [_hQ]: _m }], [1, { [_hQ]: _mk }], [0, { [_hQ]: _p }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [64 | 0, { [_hH]: _xaooa }]],
  1
];
var ListObjectsV2Output$ = [
  3,
  n0,
  _LOVO,
  { [_xN]: _LBRi },
  [_IT, _Con, _N, _P, _Deli, _MK, _CPom, _ETn, _KC, _CTon, _NCT, _SA, _RC],
  [2, [() => ObjectList, { [_xF]: 1 }], 0, 0, 0, 1, [() => CommonPrefixList, { [_xF]: 1 }], 0, 1, 0, 0, 0, [0, { [_hH]: _xarc }]]
];
var ListObjectsV2Request$ = [
  3,
  n0,
  _LOVR,
  0,
  [_B, _Deli, _ETn, _MK, _P, _CTon, _FO, _SA, _RP, _EBO, _OOA],
  [[0, 1], [0, { [_hQ]: _d }], [0, { [_hQ]: _et }], [1, { [_hQ]: _mk }], [0, { [_hQ]: _p }], [0, { [_hQ]: _ct }], [2, { [_hQ]: _fo }], [0, { [_hQ]: _sa }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [64 | 0, { [_hH]: _xaooa }]],
  1
];
var ListObjectVersionsOutput$ = [
  3,
  n0,
  _LOVOi,
  { [_xN]: _LVR },
  [_IT, _KM, _VIM, _NKM, _NVIM, _Ve, _DMe, _N, _P, _Deli, _MK, _CPom, _ETn, _RC],
  [2, 0, 0, 0, 0, [() => ObjectVersionList, { [_xF]: 1, [_xN]: _Ver }], [() => DeleteMarkers, { [_xF]: 1, [_xN]: _DM }], 0, 0, 0, 1, [() => CommonPrefixList, { [_xF]: 1 }], 0, [0, { [_hH]: _xarc }]]
];
var ListObjectVersionsRequest$ = [
  3,
  n0,
  _LOVRi,
  0,
  [_B, _Deli, _ETn, _KM, _MK, _P, _VIM, _EBO, _RP, _OOA],
  [[0, 1], [0, { [_hQ]: _d }], [0, { [_hQ]: _et }], [0, { [_hQ]: _km }], [1, { [_hQ]: _mk }], [0, { [_hQ]: _p }], [0, { [_hQ]: _vim }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xarp }], [64 | 0, { [_hH]: _xaooa }]],
  1
];
var ListPartsOutput$ = [
  3,
  n0,
  _LPO,
  { [_xN]: _LPR },
  [_ADb, _ARI, _B, _K, _UI, _PNM, _NPNM, _MP, _IT, _Pa, _In, _O, _SC, _RC, _CA, _CT],
  [[4, { [_hH]: _xaad }], [0, { [_hH]: _xaari }], 0, 0, 0, 0, 0, 1, 2, [() => Parts, { [_xF]: 1, [_xN]: _Par }], () => Initiator$, () => Owner$, 0, [0, { [_hH]: _xarc }], 0, 0]
];
var ListPartsRequest$ = [
  3,
  n0,
  _LPRi,
  0,
  [_B, _K, _UI, _MP, _PNM, _RP, _EBO, _SSECA, _SSECK, _SSECKMD],
  [[0, 1], [0, 1], [0, { [_hQ]: _uI }], [1, { [_hQ]: _mp }], [0, { [_hQ]: _pnm }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }]],
  3
];
var LocationInfo$ = [
  3,
  n0,
  _LI,
  0,
  [_Ty, _N],
  [0, 0]
];
var LoggingEnabled$ = [
  3,
  n0,
  _LE,
  0,
  [_TB, _TP, _TG, _TOKF],
  [0, 0, [() => TargetGrants, 0], [() => TargetObjectKeyFormat$, 0]],
  2
];
var MetadataConfiguration$ = [
  3,
  n0,
  _MC,
  0,
  [_JTC, _ITCn],
  [() => JournalTableConfiguration$, () => InventoryTableConfiguration$],
  1
];
var MetadataConfigurationResult$ = [
  3,
  n0,
  _MCR,
  0,
  [_DRes, _JTCR, _ITCR],
  [() => DestinationResult$, () => JournalTableConfigurationResult$, () => InventoryTableConfigurationResult$],
  1
];
var MetadataEntry$ = [
  3,
  n0,
  _ME,
  0,
  [_N, _V],
  [0, 0]
];
var MetadataTableConfiguration$ = [
  3,
  n0,
  _MTC,
  0,
  [_STD],
  [() => S3TablesDestination$],
  1
];
var MetadataTableConfigurationResult$ = [
  3,
  n0,
  _MTCR,
  0,
  [_STDR],
  [() => S3TablesDestinationResult$],
  1
];
var MetadataTableEncryptionConfiguration$ = [
  3,
  n0,
  _MTEC,
  0,
  [_SAs, _KKA],
  [0, 0],
  1
];
var Metrics$ = [
  3,
  n0,
  _Me,
  0,
  [_S, _ETv],
  [0, () => ReplicationTimeValue$],
  1
];
var MetricsAndOperator$ = [
  3,
  n0,
  _MAO,
  0,
  [_P, _T, _APAc],
  [0, [() => TagSet, { [_xF]: 1, [_xN]: _Ta }], 0]
];
var MetricsConfiguration$ = [
  3,
  n0,
  _MCe,
  0,
  [_I, _F],
  [0, [() => MetricsFilter$, 0]],
  1
];
var MultipartUpload$ = [
  3,
  n0,
  _MU,
  0,
  [_UI, _K, _Ini, _SC, _O, _In, _CA, _CT],
  [0, 0, 4, 0, () => Owner$, () => Initiator$, 0, 0]
];
var NoncurrentVersionExpiration$ = [
  3,
  n0,
  _NVE,
  0,
  [_ND, _NNV],
  [1, 1]
];
var NoncurrentVersionTransition$ = [
  3,
  n0,
  _NVTo,
  0,
  [_ND, _SC, _NNV],
  [1, 0, 1]
];
var NotificationConfiguration$ = [
  3,
  n0,
  _NC,
  0,
  [_TCo, _QCu, _LFCa, _EBC],
  [[() => TopicConfigurationList, { [_xF]: 1, [_xN]: _TCop }], [() => QueueConfigurationList, { [_xF]: 1, [_xN]: _QCue }], [() => LambdaFunctionConfigurationList, { [_xF]: 1, [_xN]: _CFC }], () => EventBridgeConfiguration$]
];
var NotificationConfigurationFilter$ = [
  3,
  n0,
  _NCF,
  0,
  [_K],
  [[() => S3KeyFilter$, { [_xN]: _SKe }]]
];
var _Object$ = [
  3,
  n0,
  _Obj,
  0,
  [_K, _LM, _ETa, _CA, _CT, _Si, _SC, _O, _RSe],
  [0, 4, 0, [64 | 0, { [_xF]: 1 }], 0, 1, 0, () => Owner$, () => RestoreStatus$]
];
var ObjectIdentifier$ = [
  3,
  n0,
  _OI,
  0,
  [_K, _VI, _ETa, _LMT, _Si],
  [0, 0, 0, 6, 1],
  1
];
var ObjectLockConfiguration$ = [
  3,
  n0,
  _OLC,
  0,
  [_OLE, _Ru],
  [0, () => ObjectLockRule$]
];
var ObjectLockLegalHold$ = [
  3,
  n0,
  _OLLH,
  0,
  [_S],
  [0]
];
var ObjectLockRetention$ = [
  3,
  n0,
  _OLR,
  0,
  [_Mo, _RUD],
  [0, 5]
];
var ObjectLockRule$ = [
  3,
  n0,
  _OLRb,
  0,
  [_DRe],
  [() => DefaultRetention$]
];
var ObjectPart$ = [
  3,
  n0,
  _OPb,
  0,
  [_PN, _Si, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe],
  [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
];
var ObjectVersion$ = [
  3,
  n0,
  _OV,
  0,
  [_ETa, _CA, _CT, _Si, _SC, _K, _VI, _IL, _LM, _O, _RSe],
  [0, [64 | 0, { [_xF]: 1 }], 0, 1, 0, 0, 0, 2, 4, () => Owner$, () => RestoreStatus$]
];
var OutputLocation$ = [
  3,
  n0,
  _OL,
  0,
  [_S_],
  [[() => S3Location$, 0]]
];
var OutputSerialization$ = [
  3,
  n0,
  _OSu,
  0,
  [_CSV, _JSON],
  [() => CSVOutput$, () => JSONOutput$]
];
var Owner$ = [
  3,
  n0,
  _O,
  0,
  [_DN, _ID],
  [0, 0]
];
var OwnershipControls$ = [
  3,
  n0,
  _OC,
  0,
  [_R],
  [[() => OwnershipControlsRules, { [_xF]: 1, [_xN]: _Ru }]],
  1
];
var OwnershipControlsRule$ = [
  3,
  n0,
  _OCR,
  0,
  [_OO],
  [0],
  1
];
var ParquetInput$ = [
  3,
  n0,
  _PI,
  0,
  [],
  []
];
var Part$ = [
  3,
  n0,
  _Par,
  0,
  [_PN, _LM, _ETa, _Si, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe],
  [1, 4, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
];
var PartitionedPrefix$ = [
  3,
  n0,
  _PP,
  { [_xN]: _PP },
  [_PDS],
  [0]
];
var PolicyStatus$ = [
  3,
  n0,
  _PS,
  0,
  [_IP],
  [[2, { [_xN]: _IP }]]
];
var Progress$ = [
  3,
  n0,
  _Pr,
  0,
  [_BS, _BP, _BRy],
  [1, 1, 1]
];
var ProgressEvent$ = [
  3,
  n0,
  _PE,
  0,
  [_Det],
  [[() => Progress$, { [_eP]: 1 }]]
];
var PublicAccessBlockConfiguration$ = [
  3,
  n0,
  _PABC,
  0,
  [_BPA, _IPA, _BPP, _RPB],
  [[2, { [_xN]: _BPA }], [2, { [_xN]: _IPA }], [2, { [_xN]: _BPP }], [2, { [_xN]: _RPB }]]
];
var PutBucketAbacRequest$ = [
  3,
  n0,
  _PBAR,
  0,
  [_B, _AS, _CMDo, _CA, _EBO],
  [[0, 1], [() => AbacStatus$, { [_hP]: 1, [_xN]: _AS }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketAccelerateConfigurationRequest$ = [
  3,
  n0,
  _PBACR,
  0,
  [_B, _AC, _EBO, _CA],
  [[0, 1], [() => AccelerateConfiguration$, { [_hP]: 1, [_xN]: _AC }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xasca }]],
  2
];
var PutBucketAclRequest$ = [
  3,
  n0,
  _PBARu,
  0,
  [_B, _ACL_, _ACP, _CMDo, _CA, _GFC, _GR, _GRACP, _GW, _GWACP, _EBO],
  [[0, 1], [0, { [_hH]: _xaa }], [() => AccessControlPolicy$, { [_hP]: 1, [_xN]: _ACP }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xagfc }], [0, { [_hH]: _xagr }], [0, { [_hH]: _xagra }], [0, { [_hH]: _xagw }], [0, { [_hH]: _xagwa }], [0, { [_hH]: _xaebo }]],
  1
];
var PutBucketAnalyticsConfigurationRequest$ = [
  3,
  n0,
  _PBACRu,
  0,
  [_B, _I, _ACn, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [() => AnalyticsConfiguration$, { [_hP]: 1, [_xN]: _ACn }], [0, { [_hH]: _xaebo }]],
  3
];
var PutBucketCorsRequest$ = [
  3,
  n0,
  _PBCR,
  0,
  [_B, _CORSC, _CMDo, _CA, _EBO],
  [[0, 1], [() => CORSConfiguration$, { [_hP]: 1, [_xN]: _CORSC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketEncryptionRequest$ = [
  3,
  n0,
  _PBER,
  0,
  [_B, _SSEC, _CMDo, _CA, _EBO],
  [[0, 1], [() => ServerSideEncryptionConfiguration$, { [_hP]: 1, [_xN]: _SSEC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketIntelligentTieringConfigurationRequest$ = [
  3,
  n0,
  _PBITCR,
  0,
  [_B, _I, _ITC, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [() => IntelligentTieringConfiguration$, { [_hP]: 1, [_xN]: _ITC }], [0, { [_hH]: _xaebo }]],
  3
];
var PutBucketInventoryConfigurationRequest$ = [
  3,
  n0,
  _PBICR,
  0,
  [_B, _I, _IC, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [() => InventoryConfiguration$, { [_hP]: 1, [_xN]: _IC }], [0, { [_hH]: _xaebo }]],
  3
];
var PutBucketLifecycleConfigurationOutput$ = [
  3,
  n0,
  _PBLCO,
  0,
  [_TDMOS],
  [[0, { [_hH]: _xatdmos }]]
];
var PutBucketLifecycleConfigurationRequest$ = [
  3,
  n0,
  _PBLCR,
  0,
  [_B, _CA, _LCi, _EBO, _TDMOS],
  [[0, 1], [0, { [_hH]: _xasca }], [() => BucketLifecycleConfiguration$, { [_hP]: 1, [_xN]: _LCi }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xatdmos }]],
  1
];
var PutBucketLoggingRequest$ = [
  3,
  n0,
  _PBLR,
  0,
  [_B, _BLS, _CMDo, _CA, _EBO],
  [[0, 1], [() => BucketLoggingStatus$, { [_hP]: 1, [_xN]: _BLS }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketMetricsConfigurationRequest$ = [
  3,
  n0,
  _PBMCR,
  0,
  [_B, _I, _MCe, _EBO],
  [[0, 1], [0, { [_hQ]: _i }], [() => MetricsConfiguration$, { [_hP]: 1, [_xN]: _MCe }], [0, { [_hH]: _xaebo }]],
  3
];
var PutBucketNotificationConfigurationRequest$ = [
  3,
  n0,
  _PBNCR,
  0,
  [_B, _NC, _EBO, _SDV],
  [[0, 1], [() => NotificationConfiguration$, { [_hP]: 1, [_xN]: _NC }], [0, { [_hH]: _xaebo }], [2, { [_hH]: _xasdv }]],
  2
];
var PutBucketOwnershipControlsRequest$ = [
  3,
  n0,
  _PBOCR,
  0,
  [_B, _OC, _CMDo, _EBO, _CA],
  [[0, 1], [() => OwnershipControls$, { [_hP]: 1, [_xN]: _OC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xasca }]],
  2
];
var PutBucketPolicyRequest$ = [
  3,
  n0,
  _PBPR,
  0,
  [_B, _Po, _CMDo, _CA, _CRSBA, _EBO],
  [[0, 1], [0, 16], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [2, { [_hH]: _xacrsba }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketReplicationRequest$ = [
  3,
  n0,
  _PBRR,
  0,
  [_B, _RCe, _CMDo, _CA, _To, _EBO],
  [[0, 1], [() => ReplicationConfiguration$, { [_hP]: 1, [_xN]: _RCe }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xabolt }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketRequestPaymentRequest$ = [
  3,
  n0,
  _PBRPR,
  0,
  [_B, _RPC, _CMDo, _CA, _EBO],
  [[0, 1], [() => RequestPaymentConfiguration$, { [_hP]: 1, [_xN]: _RPC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketTaggingRequest$ = [
  3,
  n0,
  _PBTR,
  0,
  [_B, _Tag, _CMDo, _CA, _EBO],
  [[0, 1], [() => Tagging$, { [_hP]: 1, [_xN]: _Tag }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketVersioningRequest$ = [
  3,
  n0,
  _PBVR,
  0,
  [_B, _VC, _CMDo, _CA, _MFA, _EBO],
  [[0, 1], [() => VersioningConfiguration$, { [_hP]: 1, [_xN]: _VC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xam_ }], [0, { [_hH]: _xaebo }]],
  2
];
var PutBucketWebsiteRequest$ = [
  3,
  n0,
  _PBWR,
  0,
  [_B, _WC, _CMDo, _CA, _EBO],
  [[0, 1], [() => WebsiteConfiguration$, { [_hP]: 1, [_xN]: _WC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutObjectAclOutput$ = [
  3,
  n0,
  _POAO,
  0,
  [_RC],
  [[0, { [_hH]: _xarc }]]
];
var PutObjectAclRequest$ = [
  3,
  n0,
  _POAR,
  0,
  [_B, _K, _ACL_, _ACP, _CMDo, _CA, _GFC, _GR, _GRACP, _GW, _GWACP, _RP, _VI, _EBO],
  [[0, 1], [0, 1], [0, { [_hH]: _xaa }], [() => AccessControlPolicy$, { [_hP]: 1, [_xN]: _ACP }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xagfc }], [0, { [_hH]: _xagr }], [0, { [_hH]: _xagra }], [0, { [_hH]: _xagw }], [0, { [_hH]: _xagwa }], [0, { [_hH]: _xarp }], [0, { [_hQ]: _vI }], [0, { [_hH]: _xaebo }]],
  2
];
var PutObjectLegalHoldOutput$ = [
  3,
  n0,
  _POLHO,
  0,
  [_RC],
  [[0, { [_hH]: _xarc }]]
];
var PutObjectLegalHoldRequest$ = [
  3,
  n0,
  _POLHR,
  0,
  [_B, _K, _LH, _RP, _VI, _CMDo, _CA, _EBO],
  [[0, 1], [0, 1], [() => ObjectLockLegalHold$, { [_hP]: 1, [_xN]: _LH }], [0, { [_hH]: _xarp }], [0, { [_hQ]: _vI }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutObjectLockConfigurationOutput$ = [
  3,
  n0,
  _POLCO,
  0,
  [_RC],
  [[0, { [_hH]: _xarc }]]
];
var PutObjectLockConfigurationRequest$ = [
  3,
  n0,
  _POLCR,
  0,
  [_B, _OLC, _RP, _To, _CMDo, _CA, _EBO],
  [[0, 1], [() => ObjectLockConfiguration$, { [_hP]: 1, [_xN]: _OLC }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xabolt }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  1
];
var PutObjectOutput$ = [
  3,
  n0,
  _POO,
  0,
  [_E, _ETa, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _CT, _SSE, _VI, _SSECA, _SSECKMD, _SSEKMSKI, _SSEKMSEC, _BKE, _Si, _RC],
  [[0, { [_hH]: _xae }], [0, { [_hH]: _ETa }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [0, { [_hH]: _xact }], [0, { [_hH]: _xasse }], [0, { [_hH]: _xavi }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }], [1, { [_hH]: _xaos }], [0, { [_hH]: _xarc }]]
];
var PutObjectRequest$ = [
  3,
  n0,
  _POR,
  0,
  [_B, _K, _ACL_, _Bo, _CC, _CDo, _CEo, _CL, _CLo, _CMDo, _CTo, _CA, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _Ex, _IM, _INM, _GFC, _GR, _GRACP, _GWACP, _WOB, _M, _SSE, _SC, _WRL, _SSECA, _SSECK, _SSECKMD, _SSEKMSKI, _SSEKMSEC, _BKE, _RP, _Tag, _OLM, _OLRUD, _OLLHS, _EBO],
  [[0, 1], [0, 1], [0, { [_hH]: _xaa }], [() => StreamingBlob, 16], [0, { [_hH]: _CC_ }], [0, { [_hH]: _CD_ }], [0, { [_hH]: _CE_ }], [0, { [_hH]: _CL_ }], [1, { [_hH]: _CL__ }], [0, { [_hH]: _CM }], [0, { [_hH]: _CT_ }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [4, { [_hH]: _Ex }], [0, { [_hH]: _IM_ }], [0, { [_hH]: _INM_ }], [0, { [_hH]: _xagfc }], [0, { [_hH]: _xagr }], [0, { [_hH]: _xagra }], [0, { [_hH]: _xagwa }], [1, { [_hH]: _xawob }], [128 | 0, { [_hPH]: _xam }], [0, { [_hH]: _xasse }], [0, { [_hH]: _xasc }], [0, { [_hH]: _xawrl }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [() => SSEKMSEncryptionContext, { [_hH]: _xassec }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xat }], [0, { [_hH]: _xaolm }], [5, { [_hH]: _xaolrud }], [0, { [_hH]: _xaollh }], [0, { [_hH]: _xaebo }]],
  2
];
var PutObjectRetentionOutput$ = [
  3,
  n0,
  _PORO,
  0,
  [_RC],
  [[0, { [_hH]: _xarc }]]
];
var PutObjectRetentionRequest$ = [
  3,
  n0,
  _PORR,
  0,
  [_B, _K, _Ret, _RP, _VI, _BGR, _CMDo, _CA, _EBO],
  [[0, 1], [0, 1], [() => ObjectLockRetention$, { [_hP]: 1, [_xN]: _Ret }], [0, { [_hH]: _xarp }], [0, { [_hQ]: _vI }], [2, { [_hH]: _xabgr }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var PutObjectTaggingOutput$ = [
  3,
  n0,
  _POTO,
  0,
  [_VI],
  [[0, { [_hH]: _xavi }]]
];
var PutObjectTaggingRequest$ = [
  3,
  n0,
  _POTR,
  0,
  [_B, _K, _Tag, _VI, _CMDo, _CA, _EBO, _RP],
  [[0, 1], [0, 1], [() => Tagging$, { [_hP]: 1, [_xN]: _Tag }], [0, { [_hQ]: _vI }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xarp }]],
  3
];
var PutPublicAccessBlockRequest$ = [
  3,
  n0,
  _PPABR,
  0,
  [_B, _PABC, _CMDo, _CA, _EBO],
  [[0, 1], [() => PublicAccessBlockConfiguration$, { [_hP]: 1, [_xN]: _PABC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var QueueConfiguration$ = [
  3,
  n0,
  _QCue,
  0,
  [_QA, _Ev, _I, _F],
  [[0, { [_xN]: _Qu }], [64 | 0, { [_xF]: 1, [_xN]: _Eve }], 0, [() => NotificationConfigurationFilter$, 0]],
  2
];
var RecordExpiration$ = [
  3,
  n0,
  _REe,
  0,
  [_E, _D],
  [0, 1],
  1
];
var RecordsEvent$ = [
  3,
  n0,
  _REec,
  0,
  [_Payl],
  [[21, { [_eP]: 1 }]]
];
var Redirect$ = [
  3,
  n0,
  _Red,
  0,
  [_HN, _HRC, _Pro, _RKPW, _RKW],
  [0, 0, 0, 0, 0]
];
var RedirectAllRequestsTo$ = [
  3,
  n0,
  _RART,
  0,
  [_HN, _Pro],
  [0, 0],
  1
];
var RenameObjectOutput$ = [
  3,
  n0,
  _ROO,
  0,
  [],
  []
];
var RenameObjectRequest$ = [
  3,
  n0,
  _ROR,
  0,
  [_B, _K, _RSen, _DIM, _DINM, _DIMS, _DIUS, _SIM, _SINM, _SIMS, _SIUS, _CTl],
  [[0, 1], [0, 1], [0, { [_hH]: _xars_ }], [0, { [_hH]: _IM_ }], [0, { [_hH]: _INM_ }], [4, { [_hH]: _IMS_ }], [4, { [_hH]: _IUS_ }], [0, { [_hH]: _xarsim }], [0, { [_hH]: _xarsinm }], [6, { [_hH]: _xarsims }], [6, { [_hH]: _xarsius }], [0, { [_hH]: _xact_, [_iT]: 1 }]],
  3
];
var ReplicaModifications$ = [
  3,
  n0,
  _RM,
  0,
  [_S],
  [0],
  1
];
var ReplicationConfiguration$ = [
  3,
  n0,
  _RCe,
  0,
  [_Ro, _R],
  [0, [() => ReplicationRules, { [_xF]: 1, [_xN]: _Ru }]],
  2
];
var ReplicationRule$ = [
  3,
  n0,
  _RRe,
  0,
  [_S, _Des, _ID, _Pri, _P, _F, _SSC, _EOR, _DMR],
  [0, () => Destination$, 0, 1, 0, [() => ReplicationRuleFilter$, 0], () => SourceSelectionCriteria$, () => ExistingObjectReplication$, () => DeleteMarkerReplication$],
  2
];
var ReplicationRuleAndOperator$ = [
  3,
  n0,
  _RRAO,
  0,
  [_P, _T],
  [0, [() => TagSet, { [_xF]: 1, [_xN]: _Ta }]]
];
var ReplicationRuleFilter$ = [
  3,
  n0,
  _RRF,
  0,
  [_P, _Ta, _An],
  [0, () => Tag$, [() => ReplicationRuleAndOperator$, 0]]
];
var ReplicationTime$ = [
  3,
  n0,
  _RT,
  0,
  [_S, _Tim],
  [0, () => ReplicationTimeValue$],
  2
];
var ReplicationTimeValue$ = [
  3,
  n0,
  _RTV,
  0,
  [_Mi],
  [1]
];
var RequestPaymentConfiguration$ = [
  3,
  n0,
  _RPC,
  0,
  [_Pay],
  [0],
  1
];
var RequestProgress$ = [
  3,
  n0,
  _RPe,
  0,
  [_Ena],
  [2]
];
var RestoreObjectOutput$ = [
  3,
  n0,
  _ROOe,
  0,
  [_RC, _ROP],
  [[0, { [_hH]: _xarc }], [0, { [_hH]: _xarop }]]
];
var RestoreObjectRequest$ = [
  3,
  n0,
  _RORe,
  0,
  [_B, _K, _VI, _RRes, _RP, _CA, _EBO],
  [[0, 1], [0, 1], [0, { [_hQ]: _vI }], [() => RestoreRequest$, { [_hP]: 1, [_xN]: _RRes }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var RestoreRequest$ = [
  3,
  n0,
  _RRes,
  0,
  [_D, _GJP, _Ty, _Ti, _Desc, _SP, _OL],
  [1, () => GlacierJobParameters$, 0, 0, 0, () => SelectParameters$, [() => OutputLocation$, 0]]
];
var RestoreStatus$ = [
  3,
  n0,
  _RSe,
  0,
  [_IRIP, _RED],
  [2, 4]
];
var RoutingRule$ = [
  3,
  n0,
  _RRo,
  0,
  [_Red, _Co],
  [() => Redirect$, () => Condition$],
  1
];
var S3KeyFilter$ = [
  3,
  n0,
  _SKF,
  0,
  [_FRi],
  [[() => FilterRuleList, { [_xF]: 1, [_xN]: _FR }]]
];
var S3Location$ = [
  3,
  n0,
  _SL,
  0,
  [_BNu, _P, _En, _CACL, _ACL, _Tag, _UM, _SC],
  [0, 0, [() => Encryption$, 0], 0, [() => Grants, 0], [() => Tagging$, 0], [() => UserMetadata, 0], 0],
  2
];
var S3TablesDestination$ = [
  3,
  n0,
  _STD,
  0,
  [_TBA, _TNa],
  [0, 0],
  2
];
var S3TablesDestinationResult$ = [
  3,
  n0,
  _STDR,
  0,
  [_TBA, _TNa, _TA, _TN],
  [0, 0, 0, 0],
  4
];
var ScanRange$ = [
  3,
  n0,
  _SR,
  0,
  [_St, _End],
  [1, 1]
];
var SelectObjectContentOutput$ = [
  3,
  n0,
  _SOCO,
  0,
  [_Payl],
  [[() => SelectObjectContentEventStream$, 16]]
];
var SelectObjectContentRequest$ = [
  3,
  n0,
  _SOCR,
  0,
  [_B, _K, _Exp, _ETx, _IS, _OSu, _SSECA, _SSECK, _SSECKMD, _RPe, _SR, _EBO],
  [[0, 1], [0, 1], 0, 0, () => InputSerialization$, () => OutputSerialization$, [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], () => RequestProgress$, () => ScanRange$, [0, { [_hH]: _xaebo }]],
  6
];
var SelectParameters$ = [
  3,
  n0,
  _SP,
  0,
  [_IS, _ETx, _Exp, _OSu],
  [() => InputSerialization$, 0, 0, () => OutputSerialization$],
  4
];
var ServerSideEncryptionByDefault$ = [
  3,
  n0,
  _SSEBD,
  0,
  [_SSEA, _KMSMKID],
  [0, [() => SSEKMSKeyId, 0]],
  1
];
var ServerSideEncryptionConfiguration$ = [
  3,
  n0,
  _SSEC,
  0,
  [_R],
  [[() => ServerSideEncryptionRules, { [_xF]: 1, [_xN]: _Ru }]],
  1
];
var ServerSideEncryptionRule$ = [
  3,
  n0,
  _SSER,
  0,
  [_ASSEBD, _BKE, _BET],
  [[() => ServerSideEncryptionByDefault$, 0], 2, [() => BlockedEncryptionTypes$, 0]]
];
var SessionCredentials$ = [
  3,
  n0,
  _SCe,
  0,
  [_AKI, _SAK, _ST, _E],
  [[0, { [_xN]: _AKI }], [() => SessionCredentialValue, { [_xN]: _SAK }], [() => SessionCredentialValue, { [_xN]: _ST }], [4, { [_xN]: _E }]],
  4
];
var SimplePrefix$ = [
  3,
  n0,
  _SPi,
  { [_xN]: _SPi },
  [],
  []
];
var SourceSelectionCriteria$ = [
  3,
  n0,
  _SSC,
  0,
  [_SKEO, _RM],
  [() => SseKmsEncryptedObjects$, () => ReplicaModifications$]
];
var SSEKMS$ = [
  3,
  n0,
  _SSEKMS,
  { [_xN]: _SK },
  [_KI],
  [[() => SSEKMSKeyId, 0]],
  1
];
var SseKmsEncryptedObjects$ = [
  3,
  n0,
  _SKEO,
  0,
  [_S],
  [0],
  1
];
var SSEKMSEncryption$ = [
  3,
  n0,
  _SSEKMSE,
  { [_xN]: _SK },
  [_KMSKA, _BKE],
  [[() => NonEmptyKmsKeyArnString, 0], 2],
  1
];
var SSES3$ = [
  3,
  n0,
  _SSES,
  { [_xN]: _SS },
  [],
  []
];
var Stats$ = [
  3,
  n0,
  _Sta,
  0,
  [_BS, _BP, _BRy],
  [1, 1, 1]
];
var StatsEvent$ = [
  3,
  n0,
  _SE,
  0,
  [_Det],
  [[() => Stats$, { [_eP]: 1 }]]
];
var StorageClassAnalysis$ = [
  3,
  n0,
  _SCA,
  0,
  [_DE],
  [() => StorageClassAnalysisDataExport$]
];
var StorageClassAnalysisDataExport$ = [
  3,
  n0,
  _SCADE,
  0,
  [_OSV, _Des],
  [0, () => AnalyticsExportDestination$],
  2
];
var Tag$ = [
  3,
  n0,
  _Ta,
  0,
  [_K, _V],
  [0, 0],
  2
];
var Tagging$ = [
  3,
  n0,
  _Tag,
  0,
  [_TS],
  [[() => TagSet, 0]],
  1
];
var TargetGrant$ = [
  3,
  n0,
  _TGa,
  0,
  [_Gra, _Pe],
  [[() => Grantee$, { [_xNm]: [_x, _hi] }], 0]
];
var TargetObjectKeyFormat$ = [
  3,
  n0,
  _TOKF,
  0,
  [_SPi, _PP],
  [[() => SimplePrefix$, { [_xN]: _SPi }], [() => PartitionedPrefix$, { [_xN]: _PP }]]
];
var Tiering$ = [
  3,
  n0,
  _Tier,
  0,
  [_D, _AT],
  [1, 0],
  2
];
var TopicConfiguration$ = [
  3,
  n0,
  _TCop,
  0,
  [_TAo, _Ev, _I, _F],
  [[0, { [_xN]: _Top }], [64 | 0, { [_xF]: 1, [_xN]: _Eve }], 0, [() => NotificationConfigurationFilter$, 0]],
  2
];
var Transition$ = [
  3,
  n0,
  _Tra,
  0,
  [_Da, _D, _SC],
  [5, 1, 0]
];
var UpdateBucketMetadataInventoryTableConfigurationRequest$ = [
  3,
  n0,
  _UBMITCR,
  0,
  [_B, _ITCn, _CMDo, _CA, _EBO],
  [[0, 1], [() => InventoryTableConfigurationUpdates$, { [_hP]: 1, [_xN]: _ITCn }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var UpdateBucketMetadataJournalTableConfigurationRequest$ = [
  3,
  n0,
  _UBMJTCR,
  0,
  [_B, _JTC, _CMDo, _CA, _EBO],
  [[0, 1], [() => JournalTableConfigurationUpdates$, { [_hP]: 1, [_xN]: _JTC }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xaebo }]],
  2
];
var UpdateObjectEncryptionRequest$ = [
  3,
  n0,
  _UOER,
  0,
  [_B, _K, _OE, _VI, _RP, _EBO, _CMDo, _CA],
  [[0, 1], [0, 1], [() => ObjectEncryption$, 16], [0, { [_hQ]: _vI }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }]],
  3
];
var UpdateObjectEncryptionResponse$ = [
  3,
  n0,
  _UOERp,
  0,
  [_RC],
  [[0, { [_hH]: _xarc }]]
];
var UploadPartCopyOutput$ = [
  3,
  n0,
  _UPCO,
  0,
  [_CSVI, _CPR, _SSE, _SSECA, _SSECKMD, _SSEKMSKI, _BKE, _RC],
  [[0, { [_hH]: _xacsvi }], [() => CopyPartResult$, 16], [0, { [_hH]: _xasse }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarc }]]
];
var UploadPartCopyRequest$ = [
  3,
  n0,
  _UPCR,
  0,
  [_B, _CS, _K, _PN, _UI, _CSIM, _CSIMS, _CSINM, _CSIUS, _CSRo, _SSECA, _SSECK, _SSECKMD, _CSSSECA, _CSSSECK, _CSSSECKMD, _RP, _EBO, _ESBO],
  [[0, 1], [0, { [_hH]: _xacs___ }], [0, 1], [1, { [_hQ]: _pN }], [0, { [_hQ]: _uI }], [0, { [_hH]: _xacsim }], [4, { [_hH]: _xacsims }], [0, { [_hH]: _xacsinm }], [4, { [_hH]: _xacsius }], [0, { [_hH]: _xacsr }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [0, { [_hH]: _xacssseca }], [() => CopySourceSSECustomerKey, { [_hH]: _xacssseck }], [0, { [_hH]: _xacssseckM }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }], [0, { [_hH]: _xasebo }]],
  5
];
var UploadPartOutput$ = [
  3,
  n0,
  _UPO,
  0,
  [_SSE, _ETa, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _SSECA, _SSECKMD, _SSEKMSKI, _BKE, _RC],
  [[0, { [_hH]: _xasse }], [0, { [_hH]: _ETa }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [0, { [_hH]: _xasseca }], [0, { [_hH]: _xasseckM }], [() => SSEKMSKeyId, { [_hH]: _xasseakki }], [2, { [_hH]: _xassebke }], [0, { [_hH]: _xarc }]]
];
var UploadPartRequest$ = [
  3,
  n0,
  _UPR,
  0,
  [_B, _K, _PN, _UI, _Bo, _CLo, _CMDo, _CA, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _SSECA, _SSECK, _SSECKMD, _RP, _EBO],
  [[0, 1], [0, 1], [1, { [_hQ]: _pN }], [0, { [_hQ]: _uI }], [() => StreamingBlob, 16], [1, { [_hH]: _CL__ }], [0, { [_hH]: _CM }], [0, { [_hH]: _xasca }], [0, { [_hH]: _xacc }], [0, { [_hH]: _xacc_ }], [0, { [_hH]: _xacc__ }], [0, { [_hH]: _xacs }], [0, { [_hH]: _xacs_ }], [0, { [_hH]: _xacs__ }], [0, { [_hH]: _xacm }], [0, { [_hH]: _xacx }], [0, { [_hH]: _xacx_ }], [0, { [_hH]: _xacx__ }], [0, { [_hH]: _xasseca }], [() => SSECustomerKey, { [_hH]: _xasseck }], [0, { [_hH]: _xasseckM }], [0, { [_hH]: _xarp }], [0, { [_hH]: _xaebo }]],
  4
];
var VersioningConfiguration$ = [
  3,
  n0,
  _VC,
  0,
  [_MFAD, _S],
  [[0, { [_xN]: _MDf }], 0]
];
var WebsiteConfiguration$ = [
  3,
  n0,
  _WC,
  0,
  [_EDr, _IDn, _RART, _RR],
  [() => ErrorDocument$, () => IndexDocument$, () => RedirectAllRequestsTo$, [() => RoutingRules, 0]]
];
var WriteGetObjectResponseRequest$ = [
  3,
  n0,
  _WGORR,
  0,
  [_RReq, _RTe, _Bo, _SCt, _ECr, _EM, _AR, _CC, _CDo, _CEo, _CL, _CLo, _CR, _CTo, _CCRC, _CCRCC, _CCRCNVME, _CSHA, _CSHAh, _CSHAhe, _CMD, _CXXHASH, _CXXHASHh, _CXXHASHhe, _DM, _ETa, _Ex, _E, _LM, _MM, _M, _OLM, _OLLHS, _OLRUD, _PC, _RS, _RC, _Re, _SSE, _SSECA, _SSEKMSKI, _SSECKMD, _SC, _TC, _VI, _BKE],
  [[0, { [_hL]: 1, [_hH]: _xarr }], [0, { [_hH]: _xart }], [() => StreamingBlob, 16], [1, { [_hH]: _xafs }], [0, { [_hH]: _xafec }], [0, { [_hH]: _xafem }], [0, { [_hH]: _xafhar }], [0, { [_hH]: _xafhCC }], [0, { [_hH]: _xafhCD }], [0, { [_hH]: _xafhCE }], [0, { [_hH]: _xafhCL }], [1, { [_hH]: _CL__ }], [0, { [_hH]: _xafhCR }], [0, { [_hH]: _xafhCT }], [0, { [_hH]: _xafhxacc }], [0, { [_hH]: _xafhxacc_ }], [0, { [_hH]: _xafhxacc__ }], [0, { [_hH]: _xafhxacs }], [0, { [_hH]: _xafhxacs_ }], [0, { [_hH]: _xafhxacs__ }], [0, { [_hH]: _xafhxacm }], [0, { [_hH]: _xafhxacx }], [0, { [_hH]: _xafhxacx_ }], [0, { [_hH]: _xafhxacx__ }], [2, { [_hH]: _xafhxadm }], [0, { [_hH]: _xafhE }], [4, { [_hH]: _xafhE_ }], [0, { [_hH]: _xafhxae }], [4, { [_hH]: _xafhLM }], [1, { [_hH]: _xafhxamm }], [128 | 0, { [_hPH]: _xam }], [0, { [_hH]: _xafhxaolm }], [0, { [_hH]: _xafhxaollh }], [5, { [_hH]: _xafhxaolrud }], [1, { [_hH]: _xafhxampc }], [0, { [_hH]: _xafhxars }], [0, { [_hH]: _xafhxarc }], [0, { [_hH]: _xafhxar }], [0, { [_hH]: _xafhxasse }], [0, { [_hH]: _xafhxasseca }], [() => SSEKMSKeyId, { [_hH]: _xafhxasseakki }], [0, { [_hH]: _xafhxasseckM }], [0, { [_hH]: _xafhxasc }], [1, { [_hH]: _xafhxatc }], [0, { [_hH]: _xafhxavi }], [2, { [_hH]: _xafhxassebke }]],
  2
];
var __Unit = "unit";
var AllowedHeaders = 64 | 0;
var AllowedMethods = 64 | 0;
var AllowedOrigins = 64 | 0;
var AnalyticsConfigurationList = [
  1,
  n0,
  _ACLn,
  0,
  [
    () => AnalyticsConfiguration$,
    0
  ]
];
var Buckets = [
  1,
  n0,
  _Bu,
  0,
  [
    () => Bucket$,
    { [_xN]: _B }
  ]
];
var ChecksumAlgorithmList = 64 | 0;
var CommonPrefixList = [
  1,
  n0,
  _CPL,
  0,
  () => CommonPrefix$
];
var CompletedPartList = [
  1,
  n0,
  _CPLo,
  0,
  () => CompletedPart$
];
var CORSRules = [
  1,
  n0,
  _CORSR,
  0,
  [
    () => CORSRule$,
    0
  ]
];
var DeletedObjects = [
  1,
  n0,
  _DOe,
  0,
  () => DeletedObject$
];
var DeleteMarkers = [
  1,
  n0,
  _DMe,
  0,
  () => DeleteMarkerEntry$
];
var EncryptionTypeList = [
  1,
  n0,
  _ETL,
  0,
  [
    0,
    { [_xN]: _ET }
  ]
];
var Errors = [
  1,
  n0,
  _Er,
  0,
  () => _Error$
];
var EventList = 64 | 0;
var ExposeHeaders = 64 | 0;
var FilterRuleList = [
  1,
  n0,
  _FRL,
  0,
  () => FilterRule$
];
var Grants = [
  1,
  n0,
  _G,
  0,
  [
    () => Grant$,
    { [_xN]: _Gr }
  ]
];
var IntelligentTieringConfigurationList = [
  1,
  n0,
  _ITCL,
  0,
  [
    () => IntelligentTieringConfiguration$,
    0
  ]
];
var InventoryConfigurationList = [
  1,
  n0,
  _ICL,
  0,
  [
    () => InventoryConfiguration$,
    0
  ]
];
var InventoryOptionalFields = [
  1,
  n0,
  _IOF,
  0,
  [
    0,
    { [_xN]: _Fi }
  ]
];
var LambdaFunctionConfigurationList = [
  1,
  n0,
  _LFCL,
  0,
  [
    () => LambdaFunctionConfiguration$,
    0
  ]
];
var LifecycleRules = [
  1,
  n0,
  _LRi,
  0,
  [
    () => LifecycleRule$,
    0
  ]
];
var MetricsConfigurationList = [
  1,
  n0,
  _MCL,
  0,
  [
    () => MetricsConfiguration$,
    0
  ]
];
var MultipartUploadList = [
  1,
  n0,
  _MUL,
  0,
  () => MultipartUpload$
];
var NoncurrentVersionTransitionList = [
  1,
  n0,
  _NVTL,
  0,
  () => NoncurrentVersionTransition$
];
var ObjectAttributesList = 64 | 0;
var ObjectIdentifierList = [
  1,
  n0,
  _OIL,
  0,
  () => ObjectIdentifier$
];
var ObjectList = [
  1,
  n0,
  _OLb,
  0,
  [
    () => _Object$,
    0
  ]
];
var ObjectVersionList = [
  1,
  n0,
  _OVL,
  0,
  [
    () => ObjectVersion$,
    0
  ]
];
var OptionalObjectAttributesList = 64 | 0;
var OwnershipControlsRules = [
  1,
  n0,
  _OCRw,
  0,
  () => OwnershipControlsRule$
];
var Parts = [
  1,
  n0,
  _Pa,
  0,
  () => Part$
];
var PartsList = [
  1,
  n0,
  _PL,
  0,
  () => ObjectPart$
];
var QueueConfigurationList = [
  1,
  n0,
  _QCL,
  0,
  [
    () => QueueConfiguration$,
    0
  ]
];
var ReplicationRules = [
  1,
  n0,
  _RRep,
  0,
  [
    () => ReplicationRule$,
    0
  ]
];
var RoutingRules = [
  1,
  n0,
  _RR,
  0,
  [
    () => RoutingRule$,
    { [_xN]: _RRo }
  ]
];
var ServerSideEncryptionRules = [
  1,
  n0,
  _SSERe,
  0,
  [
    () => ServerSideEncryptionRule$,
    0
  ]
];
var TagSet = [
  1,
  n0,
  _TS,
  0,
  [
    () => Tag$,
    { [_xN]: _Ta }
  ]
];
var TargetGrants = [
  1,
  n0,
  _TG,
  0,
  [
    () => TargetGrant$,
    { [_xN]: _Gr }
  ]
];
var TieringList = [
  1,
  n0,
  _TL,
  0,
  () => Tiering$
];
var TopicConfigurationList = [
  1,
  n0,
  _TCL,
  0,
  [
    () => TopicConfiguration$,
    0
  ]
];
var TransitionList = [
  1,
  n0,
  _TLr,
  0,
  () => Transition$
];
var UserMetadata = [
  1,
  n0,
  _UM,
  0,
  [
    () => MetadataEntry$,
    { [_xN]: _ME }
  ]
];
var Metadata = 128 | 0;
var AnalyticsFilter$ = [
  4,
  n0,
  _AF,
  0,
  [_P, _Ta, _An],
  [0, () => Tag$, [() => AnalyticsAndOperator$, 0]]
];
var MetricsFilter$ = [
  4,
  n0,
  _MF,
  0,
  [_P, _Ta, _APAc, _An],
  [0, () => Tag$, 0, [() => MetricsAndOperator$, 0]]
];
var ObjectEncryption$ = [
  4,
  n0,
  _OE,
  0,
  [_SSEKMS],
  [[() => SSEKMSEncryption$, { [_xN]: _SK }]]
];
var SelectObjectContentEventStream$ = [
  4,
  n0,
  _SOCES,
  { [_st]: 1 },
  [_Rec, _Sta, _Pr, _Cont, _End],
  [[() => RecordsEvent$, 0], [() => StatsEvent$, 0], [() => ProgressEvent$, 0], () => ContinuationEvent$, () => EndEvent$]
];
var AbortMultipartUpload$ = [
  9,
  n0,
  _AMU,
  { [_h]: ["DELETE", "/{Key+}?x-id=AbortMultipartUpload", 204] },
  () => AbortMultipartUploadRequest$,
  () => AbortMultipartUploadOutput$
];
var CompleteMultipartUpload$ = [
  9,
  n0,
  _CMUo,
  { [_h]: ["POST", "/{Key+}", 200] },
  () => CompleteMultipartUploadRequest$,
  () => CompleteMultipartUploadOutput$
];
var CopyObject$ = [
  9,
  n0,
  _CO,
  { [_h]: ["PUT", "/{Key+}?x-id=CopyObject", 200] },
  () => CopyObjectRequest$,
  () => CopyObjectOutput$
];
var CreateBucket$ = [
  9,
  n0,
  _CB,
  { [_h]: ["PUT", "/", 200] },
  () => CreateBucketRequest$,
  () => CreateBucketOutput$
];
var CreateBucketMetadataConfiguration$ = [
  9,
  n0,
  _CBMC,
  { [_hC]: "-", [_h]: ["POST", "/?metadataConfiguration", 200] },
  () => CreateBucketMetadataConfigurationRequest$,
  () => __Unit
];
var CreateBucketMetadataTableConfiguration$ = [
  9,
  n0,
  _CBMTC,
  { [_hC]: "-", [_h]: ["POST", "/?metadataTable", 200] },
  () => CreateBucketMetadataTableConfigurationRequest$,
  () => __Unit
];
var CreateMultipartUpload$ = [
  9,
  n0,
  _CMUr,
  { [_h]: ["POST", "/{Key+}?uploads", 200] },
  () => CreateMultipartUploadRequest$,
  () => CreateMultipartUploadOutput$
];
var CreateSession$ = [
  9,
  n0,
  _CSr,
  { [_h]: ["GET", "/?session", 200] },
  () => CreateSessionRequest$,
  () => CreateSessionOutput$
];
var DeleteBucket$ = [
  9,
  n0,
  _DB,
  { [_h]: ["DELETE", "/", 204] },
  () => DeleteBucketRequest$,
  () => __Unit
];
var DeleteBucketAnalyticsConfiguration$ = [
  9,
  n0,
  _DBAC,
  { [_h]: ["DELETE", "/?analytics", 204] },
  () => DeleteBucketAnalyticsConfigurationRequest$,
  () => __Unit
];
var DeleteBucketCors$ = [
  9,
  n0,
  _DBC,
  { [_h]: ["DELETE", "/?cors", 204] },
  () => DeleteBucketCorsRequest$,
  () => __Unit
];
var DeleteBucketEncryption$ = [
  9,
  n0,
  _DBE,
  { [_h]: ["DELETE", "/?encryption", 204] },
  () => DeleteBucketEncryptionRequest$,
  () => __Unit
];
var DeleteBucketIntelligentTieringConfiguration$ = [
  9,
  n0,
  _DBITC,
  { [_h]: ["DELETE", "/?intelligent-tiering", 204] },
  () => DeleteBucketIntelligentTieringConfigurationRequest$,
  () => __Unit
];
var DeleteBucketInventoryConfiguration$ = [
  9,
  n0,
  _DBIC,
  { [_h]: ["DELETE", "/?inventory", 204] },
  () => DeleteBucketInventoryConfigurationRequest$,
  () => __Unit
];
var DeleteBucketLifecycle$ = [
  9,
  n0,
  _DBL,
  { [_h]: ["DELETE", "/?lifecycle", 204] },
  () => DeleteBucketLifecycleRequest$,
  () => __Unit
];
var DeleteBucketMetadataConfiguration$ = [
  9,
  n0,
  _DBMC,
  { [_h]: ["DELETE", "/?metadataConfiguration", 204] },
  () => DeleteBucketMetadataConfigurationRequest$,
  () => __Unit
];
var DeleteBucketMetadataTableConfiguration$ = [
  9,
  n0,
  _DBMTC,
  { [_h]: ["DELETE", "/?metadataTable", 204] },
  () => DeleteBucketMetadataTableConfigurationRequest$,
  () => __Unit
];
var DeleteBucketMetricsConfiguration$ = [
  9,
  n0,
  _DBMCe,
  { [_h]: ["DELETE", "/?metrics", 204] },
  () => DeleteBucketMetricsConfigurationRequest$,
  () => __Unit
];
var DeleteBucketOwnershipControls$ = [
  9,
  n0,
  _DBOC,
  { [_h]: ["DELETE", "/?ownershipControls", 204] },
  () => DeleteBucketOwnershipControlsRequest$,
  () => __Unit
];
var DeleteBucketPolicy$ = [
  9,
  n0,
  _DBP,
  { [_h]: ["DELETE", "/?policy", 204] },
  () => DeleteBucketPolicyRequest$,
  () => __Unit
];
var DeleteBucketReplication$ = [
  9,
  n0,
  _DBRe,
  { [_h]: ["DELETE", "/?replication", 204] },
  () => DeleteBucketReplicationRequest$,
  () => __Unit
];
var DeleteBucketTagging$ = [
  9,
  n0,
  _DBT,
  { [_h]: ["DELETE", "/?tagging", 204] },
  () => DeleteBucketTaggingRequest$,
  () => __Unit
];
var DeleteBucketWebsite$ = [
  9,
  n0,
  _DBW,
  { [_h]: ["DELETE", "/?website", 204] },
  () => DeleteBucketWebsiteRequest$,
  () => __Unit
];
var DeleteObject$ = [
  9,
  n0,
  _DOel,
  { [_h]: ["DELETE", "/{Key+}?x-id=DeleteObject", 204] },
  () => DeleteObjectRequest$,
  () => DeleteObjectOutput$
];
var DeleteObjects$ = [
  9,
  n0,
  _DOele,
  { [_hC]: "-", [_h]: ["POST", "/?delete", 200] },
  () => DeleteObjectsRequest$,
  () => DeleteObjectsOutput$
];
var DeleteObjectTagging$ = [
  9,
  n0,
  _DOT,
  { [_h]: ["DELETE", "/{Key+}?tagging", 204] },
  () => DeleteObjectTaggingRequest$,
  () => DeleteObjectTaggingOutput$
];
var DeletePublicAccessBlock$ = [
  9,
  n0,
  _DPAB,
  { [_h]: ["DELETE", "/?publicAccessBlock", 204] },
  () => DeletePublicAccessBlockRequest$,
  () => __Unit
];
var GetBucketAbac$ = [
  9,
  n0,
  _GBA,
  { [_h]: ["GET", "/?abac", 200] },
  () => GetBucketAbacRequest$,
  () => GetBucketAbacOutput$
];
var GetBucketAccelerateConfiguration$ = [
  9,
  n0,
  _GBAC,
  { [_h]: ["GET", "/?accelerate", 200] },
  () => GetBucketAccelerateConfigurationRequest$,
  () => GetBucketAccelerateConfigurationOutput$
];
var GetBucketAcl$ = [
  9,
  n0,
  _GBAe,
  { [_h]: ["GET", "/?acl", 200] },
  () => GetBucketAclRequest$,
  () => GetBucketAclOutput$
];
var GetBucketAnalyticsConfiguration$ = [
  9,
  n0,
  _GBACe,
  { [_h]: ["GET", "/?analytics&x-id=GetBucketAnalyticsConfiguration", 200] },
  () => GetBucketAnalyticsConfigurationRequest$,
  () => GetBucketAnalyticsConfigurationOutput$
];
var GetBucketCors$ = [
  9,
  n0,
  _GBC,
  { [_h]: ["GET", "/?cors", 200] },
  () => GetBucketCorsRequest$,
  () => GetBucketCorsOutput$
];
var GetBucketEncryption$ = [
  9,
  n0,
  _GBE,
  { [_h]: ["GET", "/?encryption", 200] },
  () => GetBucketEncryptionRequest$,
  () => GetBucketEncryptionOutput$
];
var GetBucketIntelligentTieringConfiguration$ = [
  9,
  n0,
  _GBITC,
  { [_h]: ["GET", "/?intelligent-tiering&x-id=GetBucketIntelligentTieringConfiguration", 200] },
  () => GetBucketIntelligentTieringConfigurationRequest$,
  () => GetBucketIntelligentTieringConfigurationOutput$
];
var GetBucketInventoryConfiguration$ = [
  9,
  n0,
  _GBIC,
  { [_h]: ["GET", "/?inventory&x-id=GetBucketInventoryConfiguration", 200] },
  () => GetBucketInventoryConfigurationRequest$,
  () => GetBucketInventoryConfigurationOutput$
];
var GetBucketLifecycleConfiguration$ = [
  9,
  n0,
  _GBLC,
  { [_h]: ["GET", "/?lifecycle", 200] },
  () => GetBucketLifecycleConfigurationRequest$,
  () => GetBucketLifecycleConfigurationOutput$
];
var GetBucketLocation$ = [
  9,
  n0,
  _GBL,
  { [_h]: ["GET", "/?location", 200] },
  () => GetBucketLocationRequest$,
  () => GetBucketLocationOutput$
];
var GetBucketLogging$ = [
  9,
  n0,
  _GBLe,
  { [_h]: ["GET", "/?logging", 200] },
  () => GetBucketLoggingRequest$,
  () => GetBucketLoggingOutput$
];
var GetBucketMetadataConfiguration$ = [
  9,
  n0,
  _GBMC,
  { [_h]: ["GET", "/?metadataConfiguration", 200] },
  () => GetBucketMetadataConfigurationRequest$,
  () => GetBucketMetadataConfigurationOutput$
];
var GetBucketMetadataTableConfiguration$ = [
  9,
  n0,
  _GBMTC,
  { [_h]: ["GET", "/?metadataTable", 200] },
  () => GetBucketMetadataTableConfigurationRequest$,
  () => GetBucketMetadataTableConfigurationOutput$
];
var GetBucketMetricsConfiguration$ = [
  9,
  n0,
  _GBMCe,
  { [_h]: ["GET", "/?metrics&x-id=GetBucketMetricsConfiguration", 200] },
  () => GetBucketMetricsConfigurationRequest$,
  () => GetBucketMetricsConfigurationOutput$
];
var GetBucketNotificationConfiguration$ = [
  9,
  n0,
  _GBNC,
  { [_h]: ["GET", "/?notification", 200] },
  () => GetBucketNotificationConfigurationRequest$,
  () => NotificationConfiguration$
];
var GetBucketOwnershipControls$ = [
  9,
  n0,
  _GBOC,
  { [_h]: ["GET", "/?ownershipControls", 200] },
  () => GetBucketOwnershipControlsRequest$,
  () => GetBucketOwnershipControlsOutput$
];
var GetBucketPolicy$ = [
  9,
  n0,
  _GBP,
  { [_h]: ["GET", "/?policy", 200] },
  () => GetBucketPolicyRequest$,
  () => GetBucketPolicyOutput$
];
var GetBucketPolicyStatus$ = [
  9,
  n0,
  _GBPS,
  { [_h]: ["GET", "/?policyStatus", 200] },
  () => GetBucketPolicyStatusRequest$,
  () => GetBucketPolicyStatusOutput$
];
var GetBucketReplication$ = [
  9,
  n0,
  _GBR,
  { [_h]: ["GET", "/?replication", 200] },
  () => GetBucketReplicationRequest$,
  () => GetBucketReplicationOutput$
];
var GetBucketRequestPayment$ = [
  9,
  n0,
  _GBRP,
  { [_h]: ["GET", "/?requestPayment", 200] },
  () => GetBucketRequestPaymentRequest$,
  () => GetBucketRequestPaymentOutput$
];
var GetBucketTagging$ = [
  9,
  n0,
  _GBT,
  { [_h]: ["GET", "/?tagging", 200] },
  () => GetBucketTaggingRequest$,
  () => GetBucketTaggingOutput$
];
var GetBucketVersioning$ = [
  9,
  n0,
  _GBV,
  { [_h]: ["GET", "/?versioning", 200] },
  () => GetBucketVersioningRequest$,
  () => GetBucketVersioningOutput$
];
var GetBucketWebsite$ = [
  9,
  n0,
  _GBW,
  { [_h]: ["GET", "/?website", 200] },
  () => GetBucketWebsiteRequest$,
  () => GetBucketWebsiteOutput$
];
var GetObject$ = [
  9,
  n0,
  _GO,
  { [_hC]: "-", [_h]: ["GET", "/{Key+}?x-id=GetObject", 200] },
  () => GetObjectRequest$,
  () => GetObjectOutput$
];
var GetObjectAcl$ = [
  9,
  n0,
  _GOA,
  { [_h]: ["GET", "/{Key+}?acl", 200] },
  () => GetObjectAclRequest$,
  () => GetObjectAclOutput$
];
var GetObjectAttributes$ = [
  9,
  n0,
  _GOAe,
  { [_h]: ["GET", "/{Key+}?attributes", 200] },
  () => GetObjectAttributesRequest$,
  () => GetObjectAttributesOutput$
];
var GetObjectLegalHold$ = [
  9,
  n0,
  _GOLH,
  { [_h]: ["GET", "/{Key+}?legal-hold", 200] },
  () => GetObjectLegalHoldRequest$,
  () => GetObjectLegalHoldOutput$
];
var GetObjectLockConfiguration$ = [
  9,
  n0,
  _GOLC,
  { [_h]: ["GET", "/?object-lock", 200] },
  () => GetObjectLockConfigurationRequest$,
  () => GetObjectLockConfigurationOutput$
];
var GetObjectRetention$ = [
  9,
  n0,
  _GORe,
  { [_h]: ["GET", "/{Key+}?retention", 200] },
  () => GetObjectRetentionRequest$,
  () => GetObjectRetentionOutput$
];
var GetObjectTagging$ = [
  9,
  n0,
  _GOT,
  { [_h]: ["GET", "/{Key+}?tagging", 200] },
  () => GetObjectTaggingRequest$,
  () => GetObjectTaggingOutput$
];
var GetObjectTorrent$ = [
  9,
  n0,
  _GOTe,
  { [_h]: ["GET", "/{Key+}?torrent", 200] },
  () => GetObjectTorrentRequest$,
  () => GetObjectTorrentOutput$
];
var GetPublicAccessBlock$ = [
  9,
  n0,
  _GPAB,
  { [_h]: ["GET", "/?publicAccessBlock", 200] },
  () => GetPublicAccessBlockRequest$,
  () => GetPublicAccessBlockOutput$
];
var HeadBucket$ = [
  9,
  n0,
  _HB,
  { [_h]: ["HEAD", "/", 200] },
  () => HeadBucketRequest$,
  () => HeadBucketOutput$
];
var HeadObject$ = [
  9,
  n0,
  _HO,
  { [_h]: ["HEAD", "/{Key+}", 200] },
  () => HeadObjectRequest$,
  () => HeadObjectOutput$
];
var ListBucketAnalyticsConfigurations$ = [
  9,
  n0,
  _LBAC,
  { [_h]: ["GET", "/?analytics&x-id=ListBucketAnalyticsConfigurations", 200] },
  () => ListBucketAnalyticsConfigurationsRequest$,
  () => ListBucketAnalyticsConfigurationsOutput$
];
var ListBucketIntelligentTieringConfigurations$ = [
  9,
  n0,
  _LBITC,
  { [_h]: ["GET", "/?intelligent-tiering&x-id=ListBucketIntelligentTieringConfigurations", 200] },
  () => ListBucketIntelligentTieringConfigurationsRequest$,
  () => ListBucketIntelligentTieringConfigurationsOutput$
];
var ListBucketInventoryConfigurations$ = [
  9,
  n0,
  _LBIC,
  { [_h]: ["GET", "/?inventory&x-id=ListBucketInventoryConfigurations", 200] },
  () => ListBucketInventoryConfigurationsRequest$,
  () => ListBucketInventoryConfigurationsOutput$
];
var ListBucketMetricsConfigurations$ = [
  9,
  n0,
  _LBMC,
  { [_h]: ["GET", "/?metrics&x-id=ListBucketMetricsConfigurations", 200] },
  () => ListBucketMetricsConfigurationsRequest$,
  () => ListBucketMetricsConfigurationsOutput$
];
var ListBuckets$ = [
  9,
  n0,
  _LB,
  { [_h]: ["GET", "/?x-id=ListBuckets", 200] },
  () => ListBucketsRequest$,
  () => ListBucketsOutput$
];
var ListDirectoryBuckets$ = [
  9,
  n0,
  _LDB,
  { [_h]: ["GET", "/?x-id=ListDirectoryBuckets", 200] },
  () => ListDirectoryBucketsRequest$,
  () => ListDirectoryBucketsOutput$
];
var ListMultipartUploads$ = [
  9,
  n0,
  _LMU,
  { [_h]: ["GET", "/?uploads", 200] },
  () => ListMultipartUploadsRequest$,
  () => ListMultipartUploadsOutput$
];
var ListObjects$ = [
  9,
  n0,
  _LO,
  { [_h]: ["GET", "/", 200] },
  () => ListObjectsRequest$,
  () => ListObjectsOutput$
];
var ListObjectsV2$ = [
  9,
  n0,
  _LOV,
  { [_h]: ["GET", "/?list-type=2", 200] },
  () => ListObjectsV2Request$,
  () => ListObjectsV2Output$
];
var ListObjectVersions$ = [
  9,
  n0,
  _LOVi,
  { [_h]: ["GET", "/?versions", 200] },
  () => ListObjectVersionsRequest$,
  () => ListObjectVersionsOutput$
];
var ListParts$ = [
  9,
  n0,
  _LP,
  { [_h]: ["GET", "/{Key+}?x-id=ListParts", 200] },
  () => ListPartsRequest$,
  () => ListPartsOutput$
];
var PutBucketAbac$ = [
  9,
  n0,
  _PBA,
  { [_hC]: "-", [_h]: ["PUT", "/?abac", 200] },
  () => PutBucketAbacRequest$,
  () => __Unit
];
var PutBucketAccelerateConfiguration$ = [
  9,
  n0,
  _PBAC,
  { [_hC]: "-", [_h]: ["PUT", "/?accelerate", 200] },
  () => PutBucketAccelerateConfigurationRequest$,
  () => __Unit
];
var PutBucketAcl$ = [
  9,
  n0,
  _PBAu,
  { [_hC]: "-", [_h]: ["PUT", "/?acl", 200] },
  () => PutBucketAclRequest$,
  () => __Unit
];
var PutBucketAnalyticsConfiguration$ = [
  9,
  n0,
  _PBACu,
  { [_h]: ["PUT", "/?analytics", 200] },
  () => PutBucketAnalyticsConfigurationRequest$,
  () => __Unit
];
var PutBucketCors$ = [
  9,
  n0,
  _PBC,
  { [_hC]: "-", [_h]: ["PUT", "/?cors", 200] },
  () => PutBucketCorsRequest$,
  () => __Unit
];
var PutBucketEncryption$ = [
  9,
  n0,
  _PBE,
  { [_hC]: "-", [_h]: ["PUT", "/?encryption", 200] },
  () => PutBucketEncryptionRequest$,
  () => __Unit
];
var PutBucketIntelligentTieringConfiguration$ = [
  9,
  n0,
  _PBITC,
  { [_h]: ["PUT", "/?intelligent-tiering", 200] },
  () => PutBucketIntelligentTieringConfigurationRequest$,
  () => __Unit
];
var PutBucketInventoryConfiguration$ = [
  9,
  n0,
  _PBIC,
  { [_h]: ["PUT", "/?inventory", 200] },
  () => PutBucketInventoryConfigurationRequest$,
  () => __Unit
];
var PutBucketLifecycleConfiguration$ = [
  9,
  n0,
  _PBLC,
  { [_hC]: "-", [_h]: ["PUT", "/?lifecycle", 200] },
  () => PutBucketLifecycleConfigurationRequest$,
  () => PutBucketLifecycleConfigurationOutput$
];
var PutBucketLogging$ = [
  9,
  n0,
  _PBL,
  { [_hC]: "-", [_h]: ["PUT", "/?logging", 200] },
  () => PutBucketLoggingRequest$,
  () => __Unit
];
var PutBucketMetricsConfiguration$ = [
  9,
  n0,
  _PBMC,
  { [_h]: ["PUT", "/?metrics", 200] },
  () => PutBucketMetricsConfigurationRequest$,
  () => __Unit
];
var PutBucketNotificationConfiguration$ = [
  9,
  n0,
  _PBNC,
  { [_h]: ["PUT", "/?notification", 200] },
  () => PutBucketNotificationConfigurationRequest$,
  () => __Unit
];
var PutBucketOwnershipControls$ = [
  9,
  n0,
  _PBOC,
  { [_hC]: "-", [_h]: ["PUT", "/?ownershipControls", 200] },
  () => PutBucketOwnershipControlsRequest$,
  () => __Unit
];
var PutBucketPolicy$ = [
  9,
  n0,
  _PBP,
  { [_hC]: "-", [_h]: ["PUT", "/?policy", 200] },
  () => PutBucketPolicyRequest$,
  () => __Unit
];
var PutBucketReplication$ = [
  9,
  n0,
  _PBR,
  { [_hC]: "-", [_h]: ["PUT", "/?replication", 200] },
  () => PutBucketReplicationRequest$,
  () => __Unit
];
var PutBucketRequestPayment$ = [
  9,
  n0,
  _PBRP,
  { [_hC]: "-", [_h]: ["PUT", "/?requestPayment", 200] },
  () => PutBucketRequestPaymentRequest$,
  () => __Unit
];
var PutBucketTagging$ = [
  9,
  n0,
  _PBT,
  { [_hC]: "-", [_h]: ["PUT", "/?tagging", 200] },
  () => PutBucketTaggingRequest$,
  () => __Unit
];
var PutBucketVersioning$ = [
  9,
  n0,
  _PBV,
  { [_hC]: "-", [_h]: ["PUT", "/?versioning", 200] },
  () => PutBucketVersioningRequest$,
  () => __Unit
];
var PutBucketWebsite$ = [
  9,
  n0,
  _PBW,
  { [_hC]: "-", [_h]: ["PUT", "/?website", 200] },
  () => PutBucketWebsiteRequest$,
  () => __Unit
];
var PutObject$ = [
  9,
  n0,
  _PO,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?x-id=PutObject", 200] },
  () => PutObjectRequest$,
  () => PutObjectOutput$
];
var PutObjectAcl$ = [
  9,
  n0,
  _POA,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?acl", 200] },
  () => PutObjectAclRequest$,
  () => PutObjectAclOutput$
];
var PutObjectLegalHold$ = [
  9,
  n0,
  _POLH,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?legal-hold", 200] },
  () => PutObjectLegalHoldRequest$,
  () => PutObjectLegalHoldOutput$
];
var PutObjectLockConfiguration$ = [
  9,
  n0,
  _POLC,
  { [_hC]: "-", [_h]: ["PUT", "/?object-lock", 200] },
  () => PutObjectLockConfigurationRequest$,
  () => PutObjectLockConfigurationOutput$
];
var PutObjectRetention$ = [
  9,
  n0,
  _PORu,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?retention", 200] },
  () => PutObjectRetentionRequest$,
  () => PutObjectRetentionOutput$
];
var PutObjectTagging$ = [
  9,
  n0,
  _POT,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?tagging", 200] },
  () => PutObjectTaggingRequest$,
  () => PutObjectTaggingOutput$
];
var PutPublicAccessBlock$ = [
  9,
  n0,
  _PPAB,
  { [_hC]: "-", [_h]: ["PUT", "/?publicAccessBlock", 200] },
  () => PutPublicAccessBlockRequest$,
  () => __Unit
];
var RenameObject$ = [
  9,
  n0,
  _RO,
  { [_h]: ["PUT", "/{Key+}?renameObject", 200] },
  () => RenameObjectRequest$,
  () => RenameObjectOutput$
];
var RestoreObject$ = [
  9,
  n0,
  _ROe,
  { [_hC]: "-", [_h]: ["POST", "/{Key+}?restore", 200] },
  () => RestoreObjectRequest$,
  () => RestoreObjectOutput$
];
var SelectObjectContent$ = [
  9,
  n0,
  _SOC,
  { [_h]: ["POST", "/{Key+}?select&select-type=2", 200] },
  () => SelectObjectContentRequest$,
  () => SelectObjectContentOutput$
];
var UpdateBucketMetadataInventoryTableConfiguration$ = [
  9,
  n0,
  _UBMITC,
  { [_hC]: "-", [_h]: ["PUT", "/?metadataInventoryTable", 200] },
  () => UpdateBucketMetadataInventoryTableConfigurationRequest$,
  () => __Unit
];
var UpdateBucketMetadataJournalTableConfiguration$ = [
  9,
  n0,
  _UBMJTC,
  { [_hC]: "-", [_h]: ["PUT", "/?metadataJournalTable", 200] },
  () => UpdateBucketMetadataJournalTableConfigurationRequest$,
  () => __Unit
];
var UpdateObjectEncryption$ = [
  9,
  n0,
  _UOE,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?encryption", 200] },
  () => UpdateObjectEncryptionRequest$,
  () => UpdateObjectEncryptionResponse$
];
var UploadPart$ = [
  9,
  n0,
  _UP,
  { [_hC]: "-", [_h]: ["PUT", "/{Key+}?x-id=UploadPart", 200] },
  () => UploadPartRequest$,
  () => UploadPartOutput$
];
var UploadPartCopy$ = [
  9,
  n0,
  _UPC,
  { [_h]: ["PUT", "/{Key+}?x-id=UploadPartCopy", 200] },
  () => UploadPartCopyRequest$,
  () => UploadPartCopyOutput$
];
var WriteGetObjectResponse$ = [
  9,
  n0,
  _WGOR,
  { [_en]: ["{RequestRoute}."], [_h]: ["POST", "/WriteGetObjectResponse", 200] },
  () => WriteGetObjectResponseRequest$,
  () => __Unit
];

// node_modules/@aws-sdk/client-s3/dist-es/commands/CreateSessionCommand.js
var CreateSessionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  DisableS3ExpressSessionAuth: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "CreateSession", {}).n("S3Client", "CreateSessionCommand").sc(CreateSession$).build() {
};

// node_modules/@aws-sdk/client-s3/package.json
var package_default = {
  name: "@aws-sdk/client-s3",
  description: "AWS SDK for JavaScript S3 Client for Node.js, Browser and React Native",
  version: "3.1045.0",
  scripts: {
    build: "concurrently 'yarn:build:types' 'yarn:build:es' && yarn build:cjs",
    "build:cjs": "node ../../scripts/compilation/inline client-s3",
    "build:es": "tsc -p tsconfig.es.json",
    "build:include:deps": 'yarn g:turbo run build -F="$npm_package_name"',
    "build:types": "tsc -p tsconfig.types.json",
    "build:types:downlevel": "downlevel-dts dist-types dist-types/ts3.4",
    clean: "premove dist-cjs dist-es dist-types tsconfig.cjs.tsbuildinfo tsconfig.es.tsbuildinfo tsconfig.types.tsbuildinfo",
    "extract:docs": "api-extractor run --local",
    "generate:client": "node ../../scripts/generate-clients/single-service --solo s3",
    test: "yarn g:vitest run",
    "test:browser": "node ./test/browser-build/esbuild && yarn g:vitest run -c vitest.config.browser.mts",
    "test:browser:watch": "node ./test/browser-build/esbuild && yarn g:vitest watch -c vitest.config.browser.mts",
    "test:e2e": "yarn g:vitest run -c vitest.config.e2e.mts && yarn test:browser",
    "test:e2e:watch": "yarn g:vitest watch -c vitest.config.e2e.mts",
    "test:index": "tsc --noEmit ./test/index-types.ts && node ./test/index-objects.spec.mjs",
    "test:integration": "yarn g:vitest run -c vitest.config.integ.mts",
    "test:integration:watch": "yarn g:vitest watch -c vitest.config.integ.mts",
    "test:watch": "yarn g:vitest watch"
  },
  main: "./dist-cjs/index.js",
  types: "./dist-types/index.d.ts",
  module: "./dist-es/index.js",
  sideEffects: false,
  dependencies: {
    "@aws-crypto/sha1-browser": "5.2.0",
    "@aws-crypto/sha256-browser": "5.2.0",
    "@aws-crypto/sha256-js": "5.2.0",
    "@aws-sdk/core": "^3.974.8",
    "@aws-sdk/credential-provider-node": "^3.972.39",
    "@aws-sdk/middleware-bucket-endpoint": "^3.972.10",
    "@aws-sdk/middleware-expect-continue": "^3.972.10",
    "@aws-sdk/middleware-flexible-checksums": "^3.974.16",
    "@aws-sdk/middleware-host-header": "^3.972.10",
    "@aws-sdk/middleware-location-constraint": "^3.972.10",
    "@aws-sdk/middleware-logger": "^3.972.10",
    "@aws-sdk/middleware-recursion-detection": "^3.972.11",
    "@aws-sdk/middleware-sdk-s3": "^3.972.37",
    "@aws-sdk/middleware-ssec": "^3.972.10",
    "@aws-sdk/middleware-user-agent": "^3.972.38",
    "@aws-sdk/region-config-resolver": "^3.972.13",
    "@aws-sdk/signature-v4-multi-region": "^3.996.25",
    "@aws-sdk/types": "^3.973.8",
    "@aws-sdk/util-endpoints": "^3.996.8",
    "@aws-sdk/util-user-agent-browser": "^3.972.10",
    "@aws-sdk/util-user-agent-node": "^3.973.24",
    "@smithy/config-resolver": "^4.4.17",
    "@smithy/core": "^3.23.17",
    "@smithy/eventstream-serde-browser": "^4.2.14",
    "@smithy/eventstream-serde-config-resolver": "^4.3.14",
    "@smithy/eventstream-serde-node": "^4.2.14",
    "@smithy/fetch-http-handler": "^5.3.17",
    "@smithy/hash-blob-browser": "^4.2.15",
    "@smithy/hash-node": "^4.2.14",
    "@smithy/hash-stream-node": "^4.2.14",
    "@smithy/invalid-dependency": "^4.2.14",
    "@smithy/md5-js": "^4.2.14",
    "@smithy/middleware-content-length": "^4.2.14",
    "@smithy/middleware-endpoint": "^4.4.32",
    "@smithy/middleware-retry": "^4.5.7",
    "@smithy/middleware-serde": "^4.2.20",
    "@smithy/middleware-stack": "^4.2.14",
    "@smithy/node-config-provider": "^4.3.14",
    "@smithy/node-http-handler": "^4.6.1",
    "@smithy/protocol-http": "^5.3.14",
    "@smithy/smithy-client": "^4.12.13",
    "@smithy/types": "^4.14.1",
    "@smithy/url-parser": "^4.2.14",
    "@smithy/util-base64": "^4.3.2",
    "@smithy/util-body-length-browser": "^4.2.2",
    "@smithy/util-body-length-node": "^4.2.3",
    "@smithy/util-defaults-mode-browser": "^4.3.49",
    "@smithy/util-defaults-mode-node": "^4.2.54",
    "@smithy/util-endpoints": "^3.4.2",
    "@smithy/util-middleware": "^4.2.14",
    "@smithy/util-retry": "^4.3.6",
    "@smithy/util-stream": "^4.5.25",
    "@smithy/util-utf8": "^4.2.2",
    "@smithy/util-waiter": "^4.3.0",
    tslib: "^2.6.2"
  },
  devDependencies: {
    "@aws-sdk/signature-v4-crt": "3.1045.0",
    "@smithy/snapshot-testing": "^2.0.8",
    "@tsconfig/node20": "20.1.8",
    "@types/node": "^20.14.8",
    concurrently: "7.0.0",
    "downlevel-dts": "0.10.1",
    premove: "4.0.0",
    typescript: "~5.8.3",
    vitest: "^4.0.17"
  },
  engines: {
    node: ">=20.0.0"
  },
  typesVersions: {
    "<4.5": {
      "dist-types/*": [
        "dist-types/ts3.4/*"
      ]
    }
  },
  files: [
    "dist-*/**"
  ],
  author: {
    name: "AWS SDK for JavaScript Team",
    url: "https://aws.amazon.com/javascript/"
  },
  license: "Apache-2.0",
  browser: {
    "./dist-es/runtimeConfig": "./dist-es/runtimeConfig.browser"
  },
  "react-native": {
    "./dist-es/runtimeConfig": "./dist-es/runtimeConfig.native"
  },
  homepage: "https://github.com/aws/aws-sdk-js-v3/tree/main/clients/client-s3",
  repository: {
    type: "git",
    url: "https://github.com/aws/aws-sdk-js-v3.git",
    directory: "clients/client-s3"
  }
};

// node_modules/@aws-crypto/sha1-browser/build/module/isEmptyData.js
function isEmptyData2(data2) {
  if (typeof data2 === "string") {
    return data2.length === 0;
  }
  return data2.byteLength === 0;
}

// node_modules/@aws-crypto/sha1-browser/build/module/constants.js
var SHA_1_HASH = { name: "SHA-1" };
var SHA_1_HMAC_ALGO = {
  name: "HMAC",
  hash: SHA_1_HASH
};
var EMPTY_DATA_SHA_1 = new Uint8Array([
  218,
  57,
  163,
  238,
  94,
  107,
  75,
  13,
  50,
  85,
  191,
  239,
  149,
  96,
  24,
  144,
  175,
  216,
  7,
  9
]);

// node_modules/@aws-sdk/util-locate-window/dist-es/index.js
var fallbackWindow = {};
function locateWindow() {
  if (typeof window !== "undefined") {
    return window;
  } else if (typeof self !== "undefined") {
    return self;
  }
  return fallbackWindow;
}

// node_modules/@aws-crypto/sha1-browser/build/module/webCryptoSha1.js
var Sha1 = (
  /** @class */
  function() {
    function Sha13(secret) {
      this.toHash = new Uint8Array(0);
      if (secret !== void 0) {
        this.key = new Promise(function(resolve, reject) {
          locateWindow().crypto.subtle.importKey("raw", convertToBuffer2(secret), SHA_1_HMAC_ALGO, false, ["sign"]).then(resolve, reject);
        });
        this.key.catch(function() {
        });
      }
    }
    Sha13.prototype.update = function(data2) {
      if (isEmptyData2(data2)) {
        return;
      }
      var update = convertToBuffer2(data2);
      var typedArray = new Uint8Array(this.toHash.byteLength + update.byteLength);
      typedArray.set(this.toHash, 0);
      typedArray.set(update, this.toHash.byteLength);
      this.toHash = typedArray;
    };
    Sha13.prototype.digest = function() {
      var _this = this;
      if (this.key) {
        return this.key.then(function(key) {
          return locateWindow().crypto.subtle.sign(SHA_1_HMAC_ALGO, key, _this.toHash).then(function(data2) {
            return new Uint8Array(data2);
          });
        });
      }
      if (isEmptyData2(this.toHash)) {
        return Promise.resolve(EMPTY_DATA_SHA_1);
      }
      return Promise.resolve().then(function() {
        return locateWindow().crypto.subtle.digest(SHA_1_HASH, _this.toHash);
      }).then(function(data2) {
        return Promise.resolve(new Uint8Array(data2));
      });
    };
    Sha13.prototype.reset = function() {
      this.toHash = new Uint8Array(0);
    };
    return Sha13;
  }()
);
function convertToBuffer2(data2) {
  if (typeof data2 === "string") {
    return fromUtf8(data2);
  }
  if (ArrayBuffer.isView(data2)) {
    return new Uint8Array(data2.buffer, data2.byteOffset, data2.byteLength / Uint8Array.BYTES_PER_ELEMENT);
  }
  return new Uint8Array(data2);
}

// node_modules/@aws-crypto/supports-web-crypto/build/module/supportsWebCrypto.js
var subtleCryptoMethods = [
  "decrypt",
  "digest",
  "encrypt",
  "exportKey",
  "generateKey",
  "importKey",
  "sign",
  "verify"
];
function supportsWebCrypto(window2) {
  if (supportsSecureRandom(window2) && typeof window2.crypto.subtle === "object") {
    var subtle = window2.crypto.subtle;
    return supportsSubtleCrypto(subtle);
  }
  return false;
}
function supportsSecureRandom(window2) {
  if (typeof window2 === "object" && typeof window2.crypto === "object") {
    var getRandomValues = window2.crypto.getRandomValues;
    return typeof getRandomValues === "function";
  }
  return false;
}
function supportsSubtleCrypto(subtle) {
  return subtle && subtleCryptoMethods.every(function(methodName) {
    return typeof subtle[methodName] === "function";
  });
}

// node_modules/@aws-crypto/sha1-browser/build/module/crossPlatformSha1.js
var Sha12 = (
  /** @class */
  function() {
    function Sha13(secret) {
      if (supportsWebCrypto(locateWindow())) {
        this.hash = new Sha1(secret);
      } else {
        throw new Error("SHA1 not supported");
      }
    }
    Sha13.prototype.update = function(data2, encoding) {
      this.hash.update(convertToBuffer(data2));
    };
    Sha13.prototype.digest = function() {
      return this.hash.digest();
    };
    Sha13.prototype.reset = function() {
      this.hash.reset();
    };
    return Sha13;
  }()
);

// node_modules/@aws-crypto/sha256-browser/build/module/constants.js
var SHA_256_HASH = { name: "SHA-256" };
var SHA_256_HMAC_ALGO = {
  name: "HMAC",
  hash: SHA_256_HASH
};
var EMPTY_DATA_SHA_256 = new Uint8Array([
  227,
  176,
  196,
  66,
  152,
  252,
  28,
  20,
  154,
  251,
  244,
  200,
  153,
  111,
  185,
  36,
  39,
  174,
  65,
  228,
  100,
  155,
  147,
  76,
  164,
  149,
  153,
  27,
  120,
  82,
  184,
  85
]);

// node_modules/@aws-crypto/sha256-browser/build/module/webCryptoSha256.js
var Sha2562 = (
  /** @class */
  function() {
    function Sha2564(secret) {
      this.toHash = new Uint8Array(0);
      this.secret = secret;
      this.reset();
    }
    Sha2564.prototype.update = function(data2) {
      if (isEmptyData(data2)) {
        return;
      }
      var update = convertToBuffer(data2);
      var typedArray = new Uint8Array(this.toHash.byteLength + update.byteLength);
      typedArray.set(this.toHash, 0);
      typedArray.set(update, this.toHash.byteLength);
      this.toHash = typedArray;
    };
    Sha2564.prototype.digest = function() {
      var _this = this;
      if (this.key) {
        return this.key.then(function(key) {
          return locateWindow().crypto.subtle.sign(SHA_256_HMAC_ALGO, key, _this.toHash).then(function(data2) {
            return new Uint8Array(data2);
          });
        });
      }
      if (isEmptyData(this.toHash)) {
        return Promise.resolve(EMPTY_DATA_SHA_256);
      }
      return Promise.resolve().then(function() {
        return locateWindow().crypto.subtle.digest(SHA_256_HASH, _this.toHash);
      }).then(function(data2) {
        return Promise.resolve(new Uint8Array(data2));
      });
    };
    Sha2564.prototype.reset = function() {
      var _this = this;
      this.toHash = new Uint8Array(0);
      if (this.secret && this.secret !== void 0) {
        this.key = new Promise(function(resolve, reject) {
          locateWindow().crypto.subtle.importKey("raw", convertToBuffer(_this.secret), SHA_256_HMAC_ALGO, false, ["sign"]).then(resolve, reject);
        });
        this.key.catch(function() {
        });
      }
    };
    return Sha2564;
  }()
);

// node_modules/@aws-crypto/sha256-browser/build/module/crossPlatformSha256.js
var Sha2563 = (
  /** @class */
  function() {
    function Sha2564(secret) {
      if (supportsWebCrypto(locateWindow())) {
        this.hash = new Sha2562(secret);
      } else {
        this.hash = new Sha256(secret);
      }
    }
    Sha2564.prototype.update = function(data2, encoding) {
      this.hash.update(convertToBuffer(data2));
    };
    Sha2564.prototype.digest = function() {
      return this.hash.digest();
    };
    Sha2564.prototype.reset = function() {
      this.hash.reset();
    };
    return Sha2564;
  }()
);

// node_modules/@aws-sdk/util-user-agent-browser/dist-es/index.js
var createDefaultUserAgentProvider = ({ serviceId, clientVersion }) => (config) => __async(void 0, null, function* () {
  const navigator = typeof window !== "undefined" ? window.navigator : void 0;
  const uaString = navigator?.userAgent ?? "";
  const osName = navigator?.userAgentData?.platform ?? fallback.os(uaString) ?? "other";
  const osVersion = void 0;
  const brands = navigator?.userAgentData?.brands ?? [];
  const brand = brands[brands.length - 1];
  const browserName = brand?.brand ?? fallback.browser(uaString) ?? "unknown";
  const browserVersion = brand?.version ?? "unknown";
  const sections = [
    ["aws-sdk-js", clientVersion],
    ["ua", "2.1"],
    [`os/${osName}`, osVersion],
    ["lang/js"],
    ["md/browser", `${browserName}_${browserVersion}`]
  ];
  if (serviceId) {
    sections.push([`api/${serviceId}`, clientVersion]);
  }
  const appId = yield config?.userAgentAppId?.();
  if (appId) {
    sections.push([`app/${appId}`]);
  }
  return sections;
});
var fallback = {
  os(ua) {
    if (/iPhone|iPad|iPod/.test(ua))
      return "iOS";
    if (/Macintosh|Mac OS X/.test(ua))
      return "macOS";
    if (/Windows NT/.test(ua))
      return "Windows";
    if (/Android/.test(ua))
      return "Android";
    if (/Linux/.test(ua))
      return "Linux";
    return void 0;
  },
  browser(ua) {
    if (/EdgiOS|EdgA|Edg\//.test(ua))
      return "Microsoft Edge";
    if (/Firefox\//.test(ua))
      return "Firefox";
    if (/Chrome\//.test(ua))
      return "Chrome";
    if (/Safari\//.test(ua))
      return "Safari";
    return void 0;
  }
};

// node_modules/@smithy/fetch-http-handler/dist-es/create-request.js
function createRequest(url, requestOptions) {
  return new Request(url, requestOptions);
}

// node_modules/@smithy/fetch-http-handler/dist-es/request-timeout.js
function requestTimeout(timeoutInMs = 0) {
  return new Promise((resolve, reject) => {
    if (timeoutInMs) {
      setTimeout(() => {
        const timeoutError = new Error(`Request did not complete within ${timeoutInMs} ms`);
        timeoutError.name = "TimeoutError";
        reject(timeoutError);
      }, timeoutInMs);
    }
  });
}

// node_modules/@smithy/fetch-http-handler/dist-es/fetch-http-handler.js
var keepAliveSupport = {
  supported: void 0
};
var FetchHttpHandler = class _FetchHttpHandler {
  config;
  configProvider;
  static create(instanceOrOptions) {
    if (typeof instanceOrOptions?.handle === "function") {
      return instanceOrOptions;
    }
    return new _FetchHttpHandler(instanceOrOptions);
  }
  constructor(options) {
    if (typeof options === "function") {
      this.configProvider = options().then((opts) => opts || {});
    } else {
      this.config = options ?? {};
      this.configProvider = Promise.resolve(this.config);
    }
    if (keepAliveSupport.supported === void 0) {
      keepAliveSupport.supported = Boolean(typeof Request !== "undefined" && "keepalive" in createRequest("https://[::1]"));
    }
  }
  destroy() {
  }
  handle(_0) {
    return __async(this, arguments, function* (request, { abortSignal, requestTimeout: requestTimeout2 } = {}) {
      if (!this.config) {
        this.config = yield this.configProvider;
      }
      const requestTimeoutInMs = requestTimeout2 ?? this.config.requestTimeout;
      const keepAlive = this.config.keepAlive === true;
      const credentials = this.config.credentials;
      if (abortSignal?.aborted) {
        const abortError = buildAbortError(abortSignal);
        return Promise.reject(abortError);
      }
      let path = request.path;
      const queryString = buildQueryString(request.query || {});
      if (queryString) {
        path += `?${queryString}`;
      }
      if (request.fragment) {
        path += `#${request.fragment}`;
      }
      let auth = "";
      if (request.username != null || request.password != null) {
        const username = request.username ?? "";
        const password = request.password ?? "";
        auth = `${username}:${password}@`;
      }
      const { port, method } = request;
      const url = `${request.protocol}//${auth}${request.hostname}${port ? `:${port}` : ""}${path}`;
      const body = method === "GET" || method === "HEAD" ? void 0 : request.body;
      const requestOptions = {
        body,
        headers: new Headers(request.headers),
        method,
        credentials
      };
      if (this.config?.cache) {
        requestOptions.cache = this.config.cache;
      }
      if (body) {
        requestOptions.duplex = "half";
      }
      if (typeof AbortController !== "undefined") {
        requestOptions.signal = abortSignal;
      }
      if (keepAliveSupport.supported) {
        requestOptions.keepalive = keepAlive;
      }
      if (typeof this.config.requestInit === "function") {
        Object.assign(requestOptions, this.config.requestInit(request));
      }
      let removeSignalEventListener = () => {
      };
      const fetchRequest = createRequest(url, requestOptions);
      const raceOfPromises = [
        fetch(fetchRequest).then((response) => {
          const fetchHeaders = response.headers;
          const transformedHeaders = {};
          for (const pair of fetchHeaders.entries()) {
            transformedHeaders[pair[0]] = pair[1];
          }
          const hasReadableStream = response.body != void 0;
          if (!hasReadableStream) {
            return response.blob().then((body2) => ({
              response: new HttpResponse({
                headers: transformedHeaders,
                reason: response.statusText,
                statusCode: response.status,
                body: body2
              })
            }));
          }
          return {
            response: new HttpResponse({
              headers: transformedHeaders,
              reason: response.statusText,
              statusCode: response.status,
              body: response.body
            })
          };
        }),
        requestTimeout(requestTimeoutInMs)
      ];
      if (abortSignal) {
        raceOfPromises.push(new Promise((resolve, reject) => {
          const onAbort = () => {
            const abortError = buildAbortError(abortSignal);
            reject(abortError);
          };
          if (typeof abortSignal.addEventListener === "function") {
            const signal = abortSignal;
            signal.addEventListener("abort", onAbort, { once: true });
            removeSignalEventListener = () => signal.removeEventListener("abort", onAbort);
          } else {
            abortSignal.onabort = onAbort;
          }
        }));
      }
      return Promise.race(raceOfPromises).finally(removeSignalEventListener);
    });
  }
  updateHttpClientConfig(key, value) {
    this.config = void 0;
    this.configProvider = this.configProvider.then((config) => {
      config[key] = value;
      return config;
    });
  }
  httpHandlerConfigs() {
    return this.config ?? {};
  }
};
function buildAbortError(abortSignal) {
  const reason = abortSignal && typeof abortSignal === "object" && "reason" in abortSignal ? abortSignal.reason : void 0;
  if (reason) {
    if (reason instanceof Error) {
      const abortError3 = new Error("Request aborted");
      abortError3.name = "AbortError";
      abortError3.cause = reason;
      return abortError3;
    }
    const abortError2 = new Error(String(reason));
    abortError2.name = "AbortError";
    return abortError2;
  }
  const abortError = new Error("Request aborted");
  abortError.name = "AbortError";
  return abortError;
}

// node_modules/@smithy/fetch-http-handler/dist-es/stream-collector.js
var streamCollector = (stream) => __async(void 0, null, function* () {
  if (typeof Blob === "function" && stream instanceof Blob || stream.constructor?.name === "Blob") {
    if (Blob.prototype.arrayBuffer !== void 0) {
      return new Uint8Array(yield stream.arrayBuffer());
    }
    return collectBlob(stream);
  }
  return collectStream(stream);
});
function collectBlob(blob) {
  return __async(this, null, function* () {
    const base64 = yield readToBase64(blob);
    const arrayBuffer = fromBase64(base64);
    return new Uint8Array(arrayBuffer);
  });
}
function collectStream(stream) {
  return __async(this, null, function* () {
    const chunks = [];
    const reader = stream.getReader();
    let isDone = false;
    let length = 0;
    while (!isDone) {
      const { done, value } = yield reader.read();
      if (value) {
        chunks.push(value);
        length += value.length;
      }
      isDone = done;
    }
    const collected = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      collected.set(chunk, offset);
      offset += chunk.length;
    }
    return collected;
  });
}
function readToBase64(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => {
      if (reader.readyState !== 2) {
        return reject(new Error("Reader aborted too early"));
      }
      const result = reader.result ?? "";
      const commaIndex = result.indexOf(",");
      const dataOffset = commaIndex > -1 ? commaIndex + 1 : result.length;
      resolve(result.substring(dataOffset));
    };
    reader.onabort = () => reject(new Error("Read aborted"));
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(blob);
  });
}

// node_modules/@smithy/core/dist-es/submodules/checksum/chunked-blob-reader/chunked-blob-reader.js
function blobReader(_0, _1) {
  return __async(this, arguments, function* (blob, onChunk, chunkSize = 1024 * 1024) {
    const size = blob.size;
    let totalBytesRead = 0;
    while (totalBytesRead < size) {
      const slice = blob.slice(totalBytesRead, Math.min(size, totalBytesRead + chunkSize));
      onChunk(new Uint8Array(yield slice.arrayBuffer()));
      totalBytesRead += slice.size;
    }
  });
}

// node_modules/@smithy/core/dist-es/submodules/checksum/hash-blob-browser/blobHasher.js
var blobHasher = function blobHasher2(hashCtor, blob) {
  return __async(this, null, function* () {
    const hash = new hashCtor();
    yield blobReader(blob, (chunk) => {
      hash.update(chunk);
    });
    return hash.digest();
  });
};

// node_modules/@smithy/core/dist-es/submodules/checksum/md5-js/constants.js
var BLOCK_SIZE = 64;
var DIGEST_LENGTH = 16;
var INIT = [1732584193, 4023233417, 2562383102, 271733878];

// node_modules/@smithy/core/dist-es/submodules/checksum/md5-js/md5.js
var Md5 = class {
  state;
  buffer;
  bufferLength;
  bytesHashed;
  finished;
  constructor() {
    this.reset();
  }
  update(sourceData) {
    if (isEmptyData3(sourceData)) {
      return;
    } else if (this.finished) {
      throw new Error("Attempted to update an already finished hash.");
    }
    const data2 = convertToBuffer3(sourceData);
    let position = 0;
    let { byteLength } = data2;
    this.bytesHashed += byteLength;
    while (byteLength > 0) {
      this.buffer.setUint8(this.bufferLength++, data2[position++]);
      byteLength--;
      if (this.bufferLength === BLOCK_SIZE) {
        this.hashBuffer();
        this.bufferLength = 0;
      }
    }
  }
  digest() {
    return __async(this, null, function* () {
      if (!this.finished) {
        const { buffer, bufferLength: undecoratedLength, bytesHashed } = this;
        const bitsHashed = bytesHashed * 8;
        buffer.setUint8(this.bufferLength++, 128);
        if (undecoratedLength % BLOCK_SIZE >= BLOCK_SIZE - 8) {
          for (let i2 = this.bufferLength; i2 < BLOCK_SIZE; i2++) {
            buffer.setUint8(i2, 0);
          }
          this.hashBuffer();
          this.bufferLength = 0;
        }
        for (let i2 = this.bufferLength; i2 < BLOCK_SIZE - 8; i2++) {
          buffer.setUint8(i2, 0);
        }
        buffer.setUint32(BLOCK_SIZE - 8, bitsHashed >>> 0, true);
        buffer.setUint32(BLOCK_SIZE - 4, Math.floor(bitsHashed / 4294967296), true);
        this.hashBuffer();
        this.finished = true;
      }
      const out = new DataView(new ArrayBuffer(DIGEST_LENGTH));
      for (let i2 = 0; i2 < 4; i2++) {
        out.setUint32(i2 * 4, this.state[i2], true);
      }
      return new Uint8Array(out.buffer, out.byteOffset, out.byteLength);
    });
  }
  hashBuffer() {
    const { buffer, state } = this;
    let a2 = state[0], b2 = state[1], c2 = state[2], d2 = state[3];
    a2 = ff(a2, b2, c2, d2, buffer.getUint32(0, true), 7, 3614090360);
    d2 = ff(d2, a2, b2, c2, buffer.getUint32(4, true), 12, 3905402710);
    c2 = ff(c2, d2, a2, b2, buffer.getUint32(8, true), 17, 606105819);
    b2 = ff(b2, c2, d2, a2, buffer.getUint32(12, true), 22, 3250441966);
    a2 = ff(a2, b2, c2, d2, buffer.getUint32(16, true), 7, 4118548399);
    d2 = ff(d2, a2, b2, c2, buffer.getUint32(20, true), 12, 1200080426);
    c2 = ff(c2, d2, a2, b2, buffer.getUint32(24, true), 17, 2821735955);
    b2 = ff(b2, c2, d2, a2, buffer.getUint32(28, true), 22, 4249261313);
    a2 = ff(a2, b2, c2, d2, buffer.getUint32(32, true), 7, 1770035416);
    d2 = ff(d2, a2, b2, c2, buffer.getUint32(36, true), 12, 2336552879);
    c2 = ff(c2, d2, a2, b2, buffer.getUint32(40, true), 17, 4294925233);
    b2 = ff(b2, c2, d2, a2, buffer.getUint32(44, true), 22, 2304563134);
    a2 = ff(a2, b2, c2, d2, buffer.getUint32(48, true), 7, 1804603682);
    d2 = ff(d2, a2, b2, c2, buffer.getUint32(52, true), 12, 4254626195);
    c2 = ff(c2, d2, a2, b2, buffer.getUint32(56, true), 17, 2792965006);
    b2 = ff(b2, c2, d2, a2, buffer.getUint32(60, true), 22, 1236535329);
    a2 = gg(a2, b2, c2, d2, buffer.getUint32(4, true), 5, 4129170786);
    d2 = gg(d2, a2, b2, c2, buffer.getUint32(24, true), 9, 3225465664);
    c2 = gg(c2, d2, a2, b2, buffer.getUint32(44, true), 14, 643717713);
    b2 = gg(b2, c2, d2, a2, buffer.getUint32(0, true), 20, 3921069994);
    a2 = gg(a2, b2, c2, d2, buffer.getUint32(20, true), 5, 3593408605);
    d2 = gg(d2, a2, b2, c2, buffer.getUint32(40, true), 9, 38016083);
    c2 = gg(c2, d2, a2, b2, buffer.getUint32(60, true), 14, 3634488961);
    b2 = gg(b2, c2, d2, a2, buffer.getUint32(16, true), 20, 3889429448);
    a2 = gg(a2, b2, c2, d2, buffer.getUint32(36, true), 5, 568446438);
    d2 = gg(d2, a2, b2, c2, buffer.getUint32(56, true), 9, 3275163606);
    c2 = gg(c2, d2, a2, b2, buffer.getUint32(12, true), 14, 4107603335);
    b2 = gg(b2, c2, d2, a2, buffer.getUint32(32, true), 20, 1163531501);
    a2 = gg(a2, b2, c2, d2, buffer.getUint32(52, true), 5, 2850285829);
    d2 = gg(d2, a2, b2, c2, buffer.getUint32(8, true), 9, 4243563512);
    c2 = gg(c2, d2, a2, b2, buffer.getUint32(28, true), 14, 1735328473);
    b2 = gg(b2, c2, d2, a2, buffer.getUint32(48, true), 20, 2368359562);
    a2 = hh(a2, b2, c2, d2, buffer.getUint32(20, true), 4, 4294588738);
    d2 = hh(d2, a2, b2, c2, buffer.getUint32(32, true), 11, 2272392833);
    c2 = hh(c2, d2, a2, b2, buffer.getUint32(44, true), 16, 1839030562);
    b2 = hh(b2, c2, d2, a2, buffer.getUint32(56, true), 23, 4259657740);
    a2 = hh(a2, b2, c2, d2, buffer.getUint32(4, true), 4, 2763975236);
    d2 = hh(d2, a2, b2, c2, buffer.getUint32(16, true), 11, 1272893353);
    c2 = hh(c2, d2, a2, b2, buffer.getUint32(28, true), 16, 4139469664);
    b2 = hh(b2, c2, d2, a2, buffer.getUint32(40, true), 23, 3200236656);
    a2 = hh(a2, b2, c2, d2, buffer.getUint32(52, true), 4, 681279174);
    d2 = hh(d2, a2, b2, c2, buffer.getUint32(0, true), 11, 3936430074);
    c2 = hh(c2, d2, a2, b2, buffer.getUint32(12, true), 16, 3572445317);
    b2 = hh(b2, c2, d2, a2, buffer.getUint32(24, true), 23, 76029189);
    a2 = hh(a2, b2, c2, d2, buffer.getUint32(36, true), 4, 3654602809);
    d2 = hh(d2, a2, b2, c2, buffer.getUint32(48, true), 11, 3873151461);
    c2 = hh(c2, d2, a2, b2, buffer.getUint32(60, true), 16, 530742520);
    b2 = hh(b2, c2, d2, a2, buffer.getUint32(8, true), 23, 3299628645);
    a2 = ii(a2, b2, c2, d2, buffer.getUint32(0, true), 6, 4096336452);
    d2 = ii(d2, a2, b2, c2, buffer.getUint32(28, true), 10, 1126891415);
    c2 = ii(c2, d2, a2, b2, buffer.getUint32(56, true), 15, 2878612391);
    b2 = ii(b2, c2, d2, a2, buffer.getUint32(20, true), 21, 4237533241);
    a2 = ii(a2, b2, c2, d2, buffer.getUint32(48, true), 6, 1700485571);
    d2 = ii(d2, a2, b2, c2, buffer.getUint32(12, true), 10, 2399980690);
    c2 = ii(c2, d2, a2, b2, buffer.getUint32(40, true), 15, 4293915773);
    b2 = ii(b2, c2, d2, a2, buffer.getUint32(4, true), 21, 2240044497);
    a2 = ii(a2, b2, c2, d2, buffer.getUint32(32, true), 6, 1873313359);
    d2 = ii(d2, a2, b2, c2, buffer.getUint32(60, true), 10, 4264355552);
    c2 = ii(c2, d2, a2, b2, buffer.getUint32(24, true), 15, 2734768916);
    b2 = ii(b2, c2, d2, a2, buffer.getUint32(52, true), 21, 1309151649);
    a2 = ii(a2, b2, c2, d2, buffer.getUint32(16, true), 6, 4149444226);
    d2 = ii(d2, a2, b2, c2, buffer.getUint32(44, true), 10, 3174756917);
    c2 = ii(c2, d2, a2, b2, buffer.getUint32(8, true), 15, 718787259);
    b2 = ii(b2, c2, d2, a2, buffer.getUint32(36, true), 21, 3951481745);
    state[0] = a2 + state[0] & 4294967295;
    state[1] = b2 + state[1] & 4294967295;
    state[2] = c2 + state[2] & 4294967295;
    state[3] = d2 + state[3] & 4294967295;
  }
  reset() {
    this.state = Uint32Array.from(INIT);
    this.buffer = new DataView(new ArrayBuffer(BLOCK_SIZE));
    this.bufferLength = 0;
    this.bytesHashed = 0;
    this.finished = false;
  }
};
function cmn(q2, a2, b2, x2, s2, t8) {
  a2 = (a2 + q2 & 4294967295) + (x2 + t8 & 4294967295) & 4294967295;
  return (a2 << s2 | a2 >>> 32 - s2) + b2 & 4294967295;
}
function ff(a2, b2, c2, d2, x2, s2, t8) {
  return cmn(b2 & c2 | ~b2 & d2, a2, b2, x2, s2, t8);
}
function gg(a2, b2, c2, d2, x2, s2, t8) {
  return cmn(b2 & d2 | c2 & ~d2, a2, b2, x2, s2, t8);
}
function hh(a2, b2, c2, d2, x2, s2, t8) {
  return cmn(b2 ^ c2 ^ d2, a2, b2, x2, s2, t8);
}
function ii(a2, b2, c2, d2, x2, s2, t8) {
  return cmn(c2 ^ (b2 | ~d2), a2, b2, x2, s2, t8);
}
function isEmptyData3(data2) {
  if (typeof data2 === "string") {
    return data2.length === 0;
  }
  return data2.byteLength === 0;
}
function convertToBuffer3(data2) {
  if (typeof data2 === "string") {
    return fromUtf82(data2);
  }
  if (ArrayBuffer.isView(data2)) {
    return new Uint8Array(data2.buffer, data2.byteOffset, data2.byteLength / Uint8Array.BYTES_PER_ELEMENT);
  }
  return new Uint8Array(data2);
}

// node_modules/@smithy/core/dist-es/submodules/checksum/index.browser.js
var no3 = Symbol.for("node-only");

// node_modules/@smithy/util-body-length-browser/dist-es/calculateBodyLength.js
var TEXT_ENCODER = typeof TextEncoder == "function" ? new TextEncoder() : null;
var calculateBodyLength2 = (body) => {
  if (typeof body === "string") {
    if (TEXT_ENCODER) {
      return TEXT_ENCODER.encode(body).byteLength;
    }
    let len = body.length;
    for (let i2 = len - 1; i2 >= 0; i2--) {
      const code = body.charCodeAt(i2);
      if (code > 127 && code <= 2047)
        len++;
      else if (code > 2047 && code <= 65535)
        len += 2;
      if (code >= 56320 && code <= 57343)
        i2--;
    }
    return len;
  } else if (typeof body.byteLength === "number") {
    return body.byteLength;
  } else if (typeof body.size === "number") {
    return body.size;
  }
  throw new Error(`Body Length computation failed for ${body}`);
};

// node_modules/@aws-sdk/client-s3/dist-es/runtimeConfig.shared.js
var getRuntimeConfig = (config) => {
  return {
    apiVersion: "2006-03-01",
    base64Decoder: config?.base64Decoder ?? fromBase64,
    base64Encoder: config?.base64Encoder ?? toBase64,
    disableHostPrefix: config?.disableHostPrefix ?? false,
    endpointProvider: config?.endpointProvider ?? defaultEndpointResolver,
    extensions: config?.extensions ?? [],
    getAwsChunkedEncodingStream: config?.getAwsChunkedEncodingStream ?? getAwsChunkedEncodingStream,
    httpAuthSchemeProvider: config?.httpAuthSchemeProvider ?? defaultS3HttpAuthSchemeProvider,
    httpAuthSchemes: config?.httpAuthSchemes ?? [
      {
        schemeId: "aws.auth#sigv4",
        identityProvider: (ipc) => ipc.getIdentityProvider("aws.auth#sigv4"),
        signer: new AwsSdkSigV4Signer()
      },
      {
        schemeId: "aws.auth#sigv4a",
        identityProvider: (ipc) => ipc.getIdentityProvider("aws.auth#sigv4a"),
        signer: new AwsSdkSigV4ASigner()
      }
    ],
    logger: config?.logger ?? new NoOpLogger(),
    protocol: config?.protocol ?? S3RestXmlProtocol,
    protocolSettings: config?.protocolSettings ?? {
      defaultNamespace: "com.amazonaws.s3",
      errorTypeRegistries,
      xmlNamespace: "http://s3.amazonaws.com/doc/2006-03-01/",
      version: "2006-03-01",
      serviceTarget: "AmazonS3"
    },
    sdkStreamMixin: config?.sdkStreamMixin ?? sdkStreamMixin,
    serviceId: config?.serviceId ?? "S3",
    signerConstructor: config?.signerConstructor ?? SignatureV4MultiRegion,
    signingEscapePath: config?.signingEscapePath ?? false,
    urlParser: config?.urlParser ?? parseUrl,
    useArnRegion: config?.useArnRegion ?? void 0,
    utf8Decoder: config?.utf8Decoder ?? fromUtf82,
    utf8Encoder: config?.utf8Encoder ?? toUtf8
  };
};

// node_modules/@aws-sdk/client-s3/dist-es/runtimeConfig.browser.js
var getRuntimeConfig2 = (config) => {
  const defaultsMode = resolveDefaultsModeConfig(config);
  const defaultConfigProvider = () => defaultsMode().then(loadConfigsForDefaultMode);
  const clientSharedValues = getRuntimeConfig(config);
  return __spreadProps(__spreadValues(__spreadValues({}, clientSharedValues), config), {
    runtime: "browser",
    defaultsMode,
    bodyLengthChecker: config?.bodyLengthChecker ?? calculateBodyLength2,
    credentialDefaultProvider: config?.credentialDefaultProvider ?? ((_) => () => Promise.reject(new Error("Credential is missing"))),
    defaultUserAgentProvider: config?.defaultUserAgentProvider ?? createDefaultUserAgentProvider({ serviceId: clientSharedValues.serviceId, clientVersion: package_default.version }),
    eventStreamSerdeProvider: config?.eventStreamSerdeProvider ?? eventStreamSerdeProvider,
    maxAttempts: config?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
    md5: config?.md5 ?? Md5,
    region: config?.region ?? invalidProvider("Region is missing"),
    requestHandler: FetchHttpHandler.create(config?.requestHandler ?? defaultConfigProvider),
    retryMode: config?.retryMode ?? (() => __async(void 0, null, function* () {
      return (yield defaultConfigProvider()).retryMode || DEFAULT_RETRY_MODE;
    })),
    sha1: config?.sha1 ?? Sha12,
    sha256: config?.sha256 ?? Sha2563,
    streamCollector: config?.streamCollector ?? streamCollector,
    streamHasher: config?.streamHasher ?? blobHasher,
    useDualstackEndpoint: config?.useDualstackEndpoint ?? (() => Promise.resolve(DEFAULT_USE_DUALSTACK_ENDPOINT)),
    useFipsEndpoint: config?.useFipsEndpoint ?? (() => Promise.resolve(DEFAULT_USE_FIPS_ENDPOINT))
  });
};

// node_modules/@aws-sdk/region-config-resolver/dist-es/extensions/index.js
var getAwsRegionExtensionConfiguration = (runtimeConfig) => {
  return {
    setRegion(region) {
      runtimeConfig.region = region;
    },
    region() {
      return runtimeConfig.region;
    }
  };
};
var resolveAwsRegionExtensionConfiguration = (awsRegionExtensionConfiguration) => {
  return {
    region: awsRegionExtensionConfiguration.region()
  };
};

// node_modules/@aws-sdk/client-s3/dist-es/auth/httpAuthExtensionConfiguration.js
var getHttpAuthExtensionConfiguration = (runtimeConfig) => {
  const _httpAuthSchemes = runtimeConfig.httpAuthSchemes;
  let _httpAuthSchemeProvider = runtimeConfig.httpAuthSchemeProvider;
  let _credentials = runtimeConfig.credentials;
  return {
    setHttpAuthScheme(httpAuthScheme) {
      const index = _httpAuthSchemes.findIndex((scheme) => scheme.schemeId === httpAuthScheme.schemeId);
      if (index === -1) {
        _httpAuthSchemes.push(httpAuthScheme);
      } else {
        _httpAuthSchemes.splice(index, 1, httpAuthScheme);
      }
    },
    httpAuthSchemes() {
      return _httpAuthSchemes;
    },
    setHttpAuthSchemeProvider(httpAuthSchemeProvider) {
      _httpAuthSchemeProvider = httpAuthSchemeProvider;
    },
    httpAuthSchemeProvider() {
      return _httpAuthSchemeProvider;
    },
    setCredentials(credentials) {
      _credentials = credentials;
    },
    credentials() {
      return _credentials;
    }
  };
};
var resolveHttpAuthRuntimeConfig = (config) => {
  return {
    httpAuthSchemes: config.httpAuthSchemes(),
    httpAuthSchemeProvider: config.httpAuthSchemeProvider(),
    credentials: config.credentials()
  };
};

// node_modules/@aws-sdk/client-s3/dist-es/runtimeExtensions.js
var resolveRuntimeExtensions = (runtimeConfig, extensions) => {
  const extensionConfiguration = Object.assign(getAwsRegionExtensionConfiguration(runtimeConfig), getDefaultExtensionConfiguration(runtimeConfig), getHttpHandlerExtensionConfiguration(runtimeConfig), getHttpAuthExtensionConfiguration(runtimeConfig));
  extensions.forEach((extension) => extension.configure(extensionConfiguration));
  return Object.assign(runtimeConfig, resolveAwsRegionExtensionConfiguration(extensionConfiguration), resolveDefaultRuntimeConfig(extensionConfiguration), resolveHttpHandlerRuntimeConfig(extensionConfiguration), resolveHttpAuthRuntimeConfig(extensionConfiguration));
};

// node_modules/@aws-sdk/client-s3/dist-es/S3Client.js
var S3Client = class extends Client {
  config;
  constructor(...[configuration]) {
    const _config_0 = getRuntimeConfig2(configuration || {});
    super(_config_0);
    this.initConfig = _config_0;
    const _config_1 = resolveClientEndpointParameters(_config_0);
    const _config_2 = resolveUserAgentConfig(_config_1);
    const _config_3 = resolveFlexibleChecksumsConfig(_config_2);
    const _config_4 = resolveRetryConfig(_config_3);
    const _config_5 = resolveRegionConfig(_config_4);
    const _config_6 = resolveHostHeaderConfig(_config_5);
    const _config_7 = resolveEndpointConfig(_config_6);
    const _config_8 = resolveEventStreamSerdeConfig(_config_7);
    const _config_9 = resolveHttpAuthSchemeConfig(_config_8);
    const _config_10 = resolveS3Config(_config_9, { session: [() => this, CreateSessionCommand] });
    const _config_11 = resolveRuntimeExtensions(_config_10, configuration?.extensions || []);
    this.config = _config_11;
    this.middlewareStack.use(getSchemaSerdePlugin(this.config));
    this.middlewareStack.use(getUserAgentPlugin(this.config));
    this.middlewareStack.use(getRetryPlugin(this.config));
    this.middlewareStack.use(getContentLengthPlugin(this.config));
    this.middlewareStack.use(getHostHeaderPlugin(this.config));
    this.middlewareStack.use(getLoggerPlugin(this.config));
    this.middlewareStack.use(getRecursionDetectionPlugin(this.config));
    this.middlewareStack.use(getHttpAuthSchemeEndpointRuleSetPlugin(this.config, {
      httpAuthSchemeParametersProvider: defaultS3HttpAuthSchemeParametersProvider,
      identityProviderConfigProvider: (config) => __async(this, null, function* () {
        return new DefaultIdentityProviderConfig({
          "aws.auth#sigv4": config.credentials,
          "aws.auth#sigv4a": config.credentials
        });
      })
    }));
    this.middlewareStack.use(getHttpSigningPlugin(this.config));
    this.middlewareStack.use(getValidateBucketNamePlugin(this.config));
    this.middlewareStack.use(getAddExpectContinuePlugin(this.config));
    this.middlewareStack.use(getRegionRedirectMiddlewarePlugin(this.config));
    this.middlewareStack.use(getS3ExpressPlugin(this.config));
    this.middlewareStack.use(getS3ExpressHttpSigningPlugin(this.config));
  }
  destroy() {
    super.destroy();
  }
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/AbortMultipartUploadCommand.js
var AbortMultipartUploadCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "AbortMultipartUpload", {}).n("S3Client", "AbortMultipartUploadCommand").sc(AbortMultipartUpload$).build() {
};

// node_modules/@aws-sdk/middleware-ssec/dist-es/index.js
function ssecMiddleware(options) {
  return (next) => (args) => __async(this, null, function* () {
    const input = __spreadValues({}, args.input);
    const properties = [
      {
        target: "SSECustomerKey",
        hash: "SSECustomerKeyMD5"
      },
      {
        target: "CopySourceSSECustomerKey",
        hash: "CopySourceSSECustomerKeyMD5"
      }
    ];
    for (const prop of properties) {
      const value = input[prop.target];
      if (value) {
        let valueForHash;
        if (typeof value === "string") {
          if (isValidBase64EncodedSSECustomerKey(value, options)) {
            valueForHash = options.base64Decoder(value);
          } else {
            valueForHash = options.utf8Decoder(value);
            input[prop.target] = options.base64Encoder(valueForHash);
          }
        } else {
          valueForHash = ArrayBuffer.isView(value) ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength) : new Uint8Array(value);
          input[prop.target] = options.base64Encoder(valueForHash);
        }
        const hash = new options.md5();
        hash.update(valueForHash);
        input[prop.hash] = options.base64Encoder(yield hash.digest());
      }
    }
    return next(__spreadProps(__spreadValues({}, args), {
      input
    }));
  });
}
var ssecMiddlewareOptions = {
  name: "ssecMiddleware",
  step: "initialize",
  tags: ["SSE"],
  override: true
};
var getSsecPlugin = (config) => ({
  applyToStack: (clientStack) => {
    clientStack.add(ssecMiddleware(config), ssecMiddlewareOptions);
  }
});
function isValidBase64EncodedSSECustomerKey(str, options) {
  const base64Regex = /^(?:[A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
  if (!base64Regex.test(str))
    return false;
  try {
    const decodedBytes = options.base64Decoder(str);
    return decodedBytes.length === 32;
  } catch {
    return false;
  }
}

// node_modules/@aws-sdk/client-s3/dist-es/commands/CompleteMultipartUploadCommand.js
var CompleteMultipartUploadCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "CompleteMultipartUpload", {}).n("S3Client", "CompleteMultipartUploadCommand").sc(CompleteMultipartUpload$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/CopyObjectCommand.js
var CopyObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  DisableS3ExpressSessionAuth: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" },
  CopySource: { type: "contextParams", name: "CopySource" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "CopyObject", {}).n("S3Client", "CopyObjectCommand").sc(CopyObject$).build() {
};

// node_modules/@aws-sdk/middleware-location-constraint/dist-es/index.js
function locationConstraintMiddleware(options) {
  return (next) => (args) => __async(this, null, function* () {
    const { CreateBucketConfiguration } = args.input;
    const region = yield options.region();
    if (!CreateBucketConfiguration?.LocationConstraint && !CreateBucketConfiguration?.Location) {
      if (region !== "us-east-1") {
        args.input.CreateBucketConfiguration = args.input.CreateBucketConfiguration ?? {};
        args.input.CreateBucketConfiguration.LocationConstraint = region;
      }
    }
    return next(args);
  });
}
var locationConstraintMiddlewareOptions = {
  step: "initialize",
  tags: ["LOCATION_CONSTRAINT", "CREATE_BUCKET_CONFIGURATION"],
  name: "locationConstraintMiddleware",
  override: true
};
var getLocationConstraintPlugin = (config) => ({
  applyToStack: (clientStack) => {
    clientStack.add(locationConstraintMiddleware(config), locationConstraintMiddlewareOptions);
  }
});

// node_modules/@aws-sdk/client-s3/dist-es/commands/CreateBucketCommand.js
var CreateBucketCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  DisableAccessPoints: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getLocationConstraintPlugin(config)
  ];
}).s("AmazonS3", "CreateBucket", {}).n("S3Client", "CreateBucketCommand").sc(CreateBucket$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/CreateBucketMetadataConfigurationCommand.js
var CreateBucketMetadataConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "CreateBucketMetadataConfiguration", {}).n("S3Client", "CreateBucketMetadataConfigurationCommand").sc(CreateBucketMetadataConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/CreateBucketMetadataTableConfigurationCommand.js
var CreateBucketMetadataTableConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "CreateBucketMetadataTableConfiguration", {}).n("S3Client", "CreateBucketMetadataTableConfigurationCommand").sc(CreateBucketMetadataTableConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/CreateMultipartUploadCommand.js
var CreateMultipartUploadCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "CreateMultipartUpload", {}).n("S3Client", "CreateMultipartUploadCommand").sc(CreateMultipartUpload$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketAnalyticsConfigurationCommand.js
var DeleteBucketAnalyticsConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketAnalyticsConfiguration", {}).n("S3Client", "DeleteBucketAnalyticsConfigurationCommand").sc(DeleteBucketAnalyticsConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketCommand.js
var DeleteBucketCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucket", {}).n("S3Client", "DeleteBucketCommand").sc(DeleteBucket$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketCorsCommand.js
var DeleteBucketCorsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketCors", {}).n("S3Client", "DeleteBucketCorsCommand").sc(DeleteBucketCors$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketEncryptionCommand.js
var DeleteBucketEncryptionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketEncryption", {}).n("S3Client", "DeleteBucketEncryptionCommand").sc(DeleteBucketEncryption$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketIntelligentTieringConfigurationCommand.js
var DeleteBucketIntelligentTieringConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketIntelligentTieringConfiguration", {}).n("S3Client", "DeleteBucketIntelligentTieringConfigurationCommand").sc(DeleteBucketIntelligentTieringConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketInventoryConfigurationCommand.js
var DeleteBucketInventoryConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketInventoryConfiguration", {}).n("S3Client", "DeleteBucketInventoryConfigurationCommand").sc(DeleteBucketInventoryConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketLifecycleCommand.js
var DeleteBucketLifecycleCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketLifecycle", {}).n("S3Client", "DeleteBucketLifecycleCommand").sc(DeleteBucketLifecycle$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketMetadataConfigurationCommand.js
var DeleteBucketMetadataConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketMetadataConfiguration", {}).n("S3Client", "DeleteBucketMetadataConfigurationCommand").sc(DeleteBucketMetadataConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketMetadataTableConfigurationCommand.js
var DeleteBucketMetadataTableConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketMetadataTableConfiguration", {}).n("S3Client", "DeleteBucketMetadataTableConfigurationCommand").sc(DeleteBucketMetadataTableConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketMetricsConfigurationCommand.js
var DeleteBucketMetricsConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketMetricsConfiguration", {}).n("S3Client", "DeleteBucketMetricsConfigurationCommand").sc(DeleteBucketMetricsConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketOwnershipControlsCommand.js
var DeleteBucketOwnershipControlsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketOwnershipControls", {}).n("S3Client", "DeleteBucketOwnershipControlsCommand").sc(DeleteBucketOwnershipControls$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketPolicyCommand.js
var DeleteBucketPolicyCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketPolicy", {}).n("S3Client", "DeleteBucketPolicyCommand").sc(DeleteBucketPolicy$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketReplicationCommand.js
var DeleteBucketReplicationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketReplication", {}).n("S3Client", "DeleteBucketReplicationCommand").sc(DeleteBucketReplication$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketTaggingCommand.js
var DeleteBucketTaggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketTagging", {}).n("S3Client", "DeleteBucketTaggingCommand").sc(DeleteBucketTagging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteBucketWebsiteCommand.js
var DeleteBucketWebsiteCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeleteBucketWebsite", {}).n("S3Client", "DeleteBucketWebsiteCommand").sc(DeleteBucketWebsite$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteObjectCommand.js
var DeleteObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "DeleteObject", {}).n("S3Client", "DeleteObjectCommand").sc(DeleteObject$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteObjectsCommand.js
var DeleteObjectsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "DeleteObjects", {}).n("S3Client", "DeleteObjectsCommand").sc(DeleteObjects$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeleteObjectTaggingCommand.js
var DeleteObjectTaggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "DeleteObjectTagging", {}).n("S3Client", "DeleteObjectTaggingCommand").sc(DeleteObjectTagging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/DeletePublicAccessBlockCommand.js
var DeletePublicAccessBlockCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "DeletePublicAccessBlock", {}).n("S3Client", "DeletePublicAccessBlockCommand").sc(DeletePublicAccessBlock$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketAbacCommand.js
var GetBucketAbacCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketAbac", {}).n("S3Client", "GetBucketAbacCommand").sc(GetBucketAbac$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketAccelerateConfigurationCommand.js
var GetBucketAccelerateConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketAccelerateConfiguration", {}).n("S3Client", "GetBucketAccelerateConfigurationCommand").sc(GetBucketAccelerateConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketAclCommand.js
var GetBucketAclCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketAcl", {}).n("S3Client", "GetBucketAclCommand").sc(GetBucketAcl$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketAnalyticsConfigurationCommand.js
var GetBucketAnalyticsConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketAnalyticsConfiguration", {}).n("S3Client", "GetBucketAnalyticsConfigurationCommand").sc(GetBucketAnalyticsConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketCorsCommand.js
var GetBucketCorsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketCors", {}).n("S3Client", "GetBucketCorsCommand").sc(GetBucketCors$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketEncryptionCommand.js
var GetBucketEncryptionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketEncryption", {}).n("S3Client", "GetBucketEncryptionCommand").sc(GetBucketEncryption$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketIntelligentTieringConfigurationCommand.js
var GetBucketIntelligentTieringConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketIntelligentTieringConfiguration", {}).n("S3Client", "GetBucketIntelligentTieringConfigurationCommand").sc(GetBucketIntelligentTieringConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketInventoryConfigurationCommand.js
var GetBucketInventoryConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketInventoryConfiguration", {}).n("S3Client", "GetBucketInventoryConfigurationCommand").sc(GetBucketInventoryConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketLifecycleConfigurationCommand.js
var GetBucketLifecycleConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketLifecycleConfiguration", {}).n("S3Client", "GetBucketLifecycleConfigurationCommand").sc(GetBucketLifecycleConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketLocationCommand.js
var GetBucketLocationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketLocation", {}).n("S3Client", "GetBucketLocationCommand").sc(GetBucketLocation$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketLoggingCommand.js
var GetBucketLoggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketLogging", {}).n("S3Client", "GetBucketLoggingCommand").sc(GetBucketLogging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketMetadataConfigurationCommand.js
var GetBucketMetadataConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketMetadataConfiguration", {}).n("S3Client", "GetBucketMetadataConfigurationCommand").sc(GetBucketMetadataConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketMetadataTableConfigurationCommand.js
var GetBucketMetadataTableConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketMetadataTableConfiguration", {}).n("S3Client", "GetBucketMetadataTableConfigurationCommand").sc(GetBucketMetadataTableConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketMetricsConfigurationCommand.js
var GetBucketMetricsConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketMetricsConfiguration", {}).n("S3Client", "GetBucketMetricsConfigurationCommand").sc(GetBucketMetricsConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketNotificationConfigurationCommand.js
var GetBucketNotificationConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketNotificationConfiguration", {}).n("S3Client", "GetBucketNotificationConfigurationCommand").sc(GetBucketNotificationConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketOwnershipControlsCommand.js
var GetBucketOwnershipControlsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketOwnershipControls", {}).n("S3Client", "GetBucketOwnershipControlsCommand").sc(GetBucketOwnershipControls$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketPolicyCommand.js
var GetBucketPolicyCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "GetBucketPolicy", {}).n("S3Client", "GetBucketPolicyCommand").sc(GetBucketPolicy$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketPolicyStatusCommand.js
var GetBucketPolicyStatusCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketPolicyStatus", {}).n("S3Client", "GetBucketPolicyStatusCommand").sc(GetBucketPolicyStatus$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketReplicationCommand.js
var GetBucketReplicationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketReplication", {}).n("S3Client", "GetBucketReplicationCommand").sc(GetBucketReplication$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketRequestPaymentCommand.js
var GetBucketRequestPaymentCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketRequestPayment", {}).n("S3Client", "GetBucketRequestPaymentCommand").sc(GetBucketRequestPayment$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketTaggingCommand.js
var GetBucketTaggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketTagging", {}).n("S3Client", "GetBucketTaggingCommand").sc(GetBucketTagging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketVersioningCommand.js
var GetBucketVersioningCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketVersioning", {}).n("S3Client", "GetBucketVersioningCommand").sc(GetBucketVersioning$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetBucketWebsiteCommand.js
var GetBucketWebsiteCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetBucketWebsite", {}).n("S3Client", "GetBucketWebsiteCommand").sc(GetBucketWebsite$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectAclCommand.js
var GetObjectAclCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetObjectAcl", {}).n("S3Client", "GetObjectAclCommand").sc(GetObjectAcl$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectAttributesCommand.js
var GetObjectAttributesCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "GetObjectAttributes", {}).n("S3Client", "GetObjectAttributesCommand").sc(GetObjectAttributes$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectCommand.js
var GetObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestChecksumRequired: false,
      requestValidationModeMember: "ChecksumMode",
      "responseAlgorithms": ["CRC64NVME", "CRC32", "CRC32C", "SHA256", "SHA1", "SHA512", "MD5", "XXHASH64", "XXHASH3", "XXHASH128"]
    }),
    getSsecPlugin(config),
    getS3ExpiresMiddlewarePlugin(config)
  ];
}).s("AmazonS3", "GetObject", {}).n("S3Client", "GetObjectCommand").sc(GetObject$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectLegalHoldCommand.js
var GetObjectLegalHoldCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetObjectLegalHold", {}).n("S3Client", "GetObjectLegalHoldCommand").sc(GetObjectLegalHold$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectLockConfigurationCommand.js
var GetObjectLockConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetObjectLockConfiguration", {}).n("S3Client", "GetObjectLockConfigurationCommand").sc(GetObjectLockConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectRetentionCommand.js
var GetObjectRetentionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetObjectRetention", {}).n("S3Client", "GetObjectRetentionCommand").sc(GetObjectRetention$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectTaggingCommand.js
var GetObjectTaggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetObjectTagging", {}).n("S3Client", "GetObjectTaggingCommand").sc(GetObjectTagging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetObjectTorrentCommand.js
var GetObjectTorrentCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "GetObjectTorrent", {}).n("S3Client", "GetObjectTorrentCommand").sc(GetObjectTorrent$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/GetPublicAccessBlockCommand.js
var GetPublicAccessBlockCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "GetPublicAccessBlock", {}).n("S3Client", "GetPublicAccessBlockCommand").sc(GetPublicAccessBlock$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/HeadBucketCommand.js
var HeadBucketCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "HeadBucket", {}).n("S3Client", "HeadBucketCommand").sc(HeadBucket$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/HeadObjectCommand.js
var HeadObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config),
    getS3ExpiresMiddlewarePlugin(config)
  ];
}).s("AmazonS3", "HeadObject", {}).n("S3Client", "HeadObjectCommand").sc(HeadObject$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListBucketAnalyticsConfigurationsCommand.js
var ListBucketAnalyticsConfigurationsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListBucketAnalyticsConfigurations", {}).n("S3Client", "ListBucketAnalyticsConfigurationsCommand").sc(ListBucketAnalyticsConfigurations$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListBucketIntelligentTieringConfigurationsCommand.js
var ListBucketIntelligentTieringConfigurationsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListBucketIntelligentTieringConfigurations", {}).n("S3Client", "ListBucketIntelligentTieringConfigurationsCommand").sc(ListBucketIntelligentTieringConfigurations$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListBucketInventoryConfigurationsCommand.js
var ListBucketInventoryConfigurationsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListBucketInventoryConfigurations", {}).n("S3Client", "ListBucketInventoryConfigurationsCommand").sc(ListBucketInventoryConfigurations$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListBucketMetricsConfigurationsCommand.js
var ListBucketMetricsConfigurationsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListBucketMetricsConfigurations", {}).n("S3Client", "ListBucketMetricsConfigurationsCommand").sc(ListBucketMetricsConfigurations$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListBucketsCommand.js
var ListBucketsCommand = class extends Command.classBuilder().ep(commonParams).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListBuckets", {}).n("S3Client", "ListBucketsCommand").sc(ListBuckets$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListDirectoryBucketsCommand.js
var ListDirectoryBucketsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListDirectoryBuckets", {}).n("S3Client", "ListDirectoryBucketsCommand").sc(ListDirectoryBuckets$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListMultipartUploadsCommand.js
var ListMultipartUploadsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Prefix: { type: "contextParams", name: "Prefix" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListMultipartUploads", {}).n("S3Client", "ListMultipartUploadsCommand").sc(ListMultipartUploads$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListObjectsCommand.js
var ListObjectsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Prefix: { type: "contextParams", name: "Prefix" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListObjects", {}).n("S3Client", "ListObjectsCommand").sc(ListObjects$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListObjectsV2Command.js
var ListObjectsV2Command = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Prefix: { type: "contextParams", name: "Prefix" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListObjectsV2", {}).n("S3Client", "ListObjectsV2Command").sc(ListObjectsV2$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListObjectVersionsCommand.js
var ListObjectVersionsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Prefix: { type: "contextParams", name: "Prefix" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "ListObjectVersions", {}).n("S3Client", "ListObjectVersionsCommand").sc(ListObjectVersions$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/ListPartsCommand.js
var ListPartsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "ListParts", {}).n("S3Client", "ListPartsCommand").sc(ListParts$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketAbacCommand.js
var PutBucketAbacCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: false
    })
  ];
}).s("AmazonS3", "PutBucketAbac", {}).n("S3Client", "PutBucketAbacCommand").sc(PutBucketAbac$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketAccelerateConfigurationCommand.js
var PutBucketAccelerateConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: false
    })
  ];
}).s("AmazonS3", "PutBucketAccelerateConfiguration", {}).n("S3Client", "PutBucketAccelerateConfigurationCommand").sc(PutBucketAccelerateConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketAclCommand.js
var PutBucketAclCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketAcl", {}).n("S3Client", "PutBucketAclCommand").sc(PutBucketAcl$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketAnalyticsConfigurationCommand.js
var PutBucketAnalyticsConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "PutBucketAnalyticsConfiguration", {}).n("S3Client", "PutBucketAnalyticsConfigurationCommand").sc(PutBucketAnalyticsConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketCorsCommand.js
var PutBucketCorsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketCors", {}).n("S3Client", "PutBucketCorsCommand").sc(PutBucketCors$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketEncryptionCommand.js
var PutBucketEncryptionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketEncryption", {}).n("S3Client", "PutBucketEncryptionCommand").sc(PutBucketEncryption$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketIntelligentTieringConfigurationCommand.js
var PutBucketIntelligentTieringConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "PutBucketIntelligentTieringConfiguration", {}).n("S3Client", "PutBucketIntelligentTieringConfigurationCommand").sc(PutBucketIntelligentTieringConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketInventoryConfigurationCommand.js
var PutBucketInventoryConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "PutBucketInventoryConfiguration", {}).n("S3Client", "PutBucketInventoryConfigurationCommand").sc(PutBucketInventoryConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketLifecycleConfigurationCommand.js
var PutBucketLifecycleConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "PutBucketLifecycleConfiguration", {}).n("S3Client", "PutBucketLifecycleConfigurationCommand").sc(PutBucketLifecycleConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketLoggingCommand.js
var PutBucketLoggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketLogging", {}).n("S3Client", "PutBucketLoggingCommand").sc(PutBucketLogging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketMetricsConfigurationCommand.js
var PutBucketMetricsConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "PutBucketMetricsConfiguration", {}).n("S3Client", "PutBucketMetricsConfigurationCommand").sc(PutBucketMetricsConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketNotificationConfigurationCommand.js
var PutBucketNotificationConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "PutBucketNotificationConfiguration", {}).n("S3Client", "PutBucketNotificationConfigurationCommand").sc(PutBucketNotificationConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketOwnershipControlsCommand.js
var PutBucketOwnershipControlsCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketOwnershipControls", {}).n("S3Client", "PutBucketOwnershipControlsCommand").sc(PutBucketOwnershipControls$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketPolicyCommand.js
var PutBucketPolicyCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketPolicy", {}).n("S3Client", "PutBucketPolicyCommand").sc(PutBucketPolicy$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketReplicationCommand.js
var PutBucketReplicationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketReplication", {}).n("S3Client", "PutBucketReplicationCommand").sc(PutBucketReplication$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketRequestPaymentCommand.js
var PutBucketRequestPaymentCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketRequestPayment", {}).n("S3Client", "PutBucketRequestPaymentCommand").sc(PutBucketRequestPayment$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketTaggingCommand.js
var PutBucketTaggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketTagging", {}).n("S3Client", "PutBucketTaggingCommand").sc(PutBucketTagging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketVersioningCommand.js
var PutBucketVersioningCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketVersioning", {}).n("S3Client", "PutBucketVersioningCommand").sc(PutBucketVersioning$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutBucketWebsiteCommand.js
var PutBucketWebsiteCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutBucketWebsite", {}).n("S3Client", "PutBucketWebsiteCommand").sc(PutBucketWebsite$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutObjectAclCommand.js
var PutObjectAclCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "PutObjectAcl", {}).n("S3Client", "PutObjectAclCommand").sc(PutObjectAcl$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutObjectCommand.js
var PutObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: false
    }),
    getCheckContentLengthHeaderPlugin(config),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "PutObject", {}).n("S3Client", "PutObjectCommand").sc(PutObject$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutObjectLegalHoldCommand.js
var PutObjectLegalHoldCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "PutObjectLegalHold", {}).n("S3Client", "PutObjectLegalHoldCommand").sc(PutObjectLegalHold$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutObjectLockConfigurationCommand.js
var PutObjectLockConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "PutObjectLockConfiguration", {}).n("S3Client", "PutObjectLockConfigurationCommand").sc(PutObjectLockConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutObjectRetentionCommand.js
var PutObjectRetentionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "PutObjectRetention", {}).n("S3Client", "PutObjectRetentionCommand").sc(PutObjectRetention$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutObjectTaggingCommand.js
var PutObjectTaggingCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "PutObjectTagging", {}).n("S3Client", "PutObjectTaggingCommand").sc(PutObjectTagging$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/PutPublicAccessBlockCommand.js
var PutPublicAccessBlockCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "PutPublicAccessBlock", {}).n("S3Client", "PutPublicAccessBlockCommand").sc(PutPublicAccessBlock$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/RenameObjectCommand.js
var RenameObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "RenameObject", {}).n("S3Client", "RenameObjectCommand").sc(RenameObject$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/RestoreObjectCommand.js
var RestoreObjectCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: false
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "RestoreObject", {}).n("S3Client", "RestoreObjectCommand").sc(RestoreObject$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/SelectObjectContentCommand.js
var SelectObjectContentCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "SelectObjectContent", {
  eventStream: {
    output: true
  }
}).n("S3Client", "SelectObjectContentCommand").sc(SelectObjectContent$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/UpdateBucketMetadataInventoryTableConfigurationCommand.js
var UpdateBucketMetadataInventoryTableConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "UpdateBucketMetadataInventoryTableConfiguration", {}).n("S3Client", "UpdateBucketMetadataInventoryTableConfigurationCommand").sc(UpdateBucketMetadataInventoryTableConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/UpdateBucketMetadataJournalTableConfigurationCommand.js
var UpdateBucketMetadataJournalTableConfigurationCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseS3ExpressControlEndpoint: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    })
  ];
}).s("AmazonS3", "UpdateBucketMetadataJournalTableConfiguration", {}).n("S3Client", "UpdateBucketMetadataJournalTableConfigurationCommand").sc(UpdateBucketMetadataJournalTableConfiguration$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/UpdateObjectEncryptionCommand.js
var UpdateObjectEncryptionCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: true
    }),
    getThrow200ExceptionsPlugin(config)
  ];
}).s("AmazonS3", "UpdateObjectEncryption", {}).n("S3Client", "UpdateObjectEncryptionCommand").sc(UpdateObjectEncryption$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/UploadPartCommand.js
var UploadPartCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  Bucket: { type: "contextParams", name: "Bucket" },
  Key: { type: "contextParams", name: "Key" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getFlexibleChecksumsPlugin(config, {
      requestAlgorithmMember: { "httpHeader": "x-amz-sdk-checksum-algorithm", "name": "ChecksumAlgorithm" },
      requestChecksumRequired: false
    }),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "UploadPart", {}).n("S3Client", "UploadPartCommand").sc(UploadPart$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/UploadPartCopyCommand.js
var UploadPartCopyCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  DisableS3ExpressSessionAuth: { type: "staticContextParams", value: true },
  Bucket: { type: "contextParams", name: "Bucket" }
})).m(function(Command2, cs, config, o2) {
  return [
    getEndpointPlugin(config, Command2.getEndpointParameterInstructions()),
    getThrow200ExceptionsPlugin(config),
    getSsecPlugin(config)
  ];
}).s("AmazonS3", "UploadPartCopy", {}).n("S3Client", "UploadPartCopyCommand").sc(UploadPartCopy$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/commands/WriteGetObjectResponseCommand.js
var WriteGetObjectResponseCommand = class extends Command.classBuilder().ep(__spreadProps(__spreadValues({}, commonParams), {
  UseObjectLambdaEndpoint: { type: "staticContextParams", value: true }
})).m(function(Command2, cs, config, o2) {
  return [getEndpointPlugin(config, Command2.getEndpointParameterInstructions())];
}).s("AmazonS3", "WriteGetObjectResponse", {}).n("S3Client", "WriteGetObjectResponseCommand").sc(WriteGetObjectResponse$).build() {
};

// node_modules/@aws-sdk/client-s3/dist-es/pagination/ListBucketsPaginator.js
var paginateListBuckets = createPaginator(S3Client, ListBucketsCommand, "ContinuationToken", "ContinuationToken", "MaxBuckets");

// node_modules/@aws-sdk/client-s3/dist-es/pagination/ListDirectoryBucketsPaginator.js
var paginateListDirectoryBuckets = createPaginator(S3Client, ListDirectoryBucketsCommand, "ContinuationToken", "ContinuationToken", "MaxDirectoryBuckets");

// node_modules/@aws-sdk/client-s3/dist-es/pagination/ListObjectsV2Paginator.js
var paginateListObjectsV2 = createPaginator(S3Client, ListObjectsV2Command, "ContinuationToken", "NextContinuationToken", "MaxKeys");

// node_modules/@aws-sdk/client-s3/dist-es/pagination/ListPartsPaginator.js
var paginateListParts = createPaginator(S3Client, ListPartsCommand, "PartNumberMarker", "NextPartNumberMarker", "MaxParts");

// node_modules/@aws-sdk/client-s3/dist-es/waiters/waitForBucketExists.js
var checkState = (client, input) => __async(void 0, null, function* () {
  let reason;
  try {
    let result = yield client.send(new HeadBucketCommand(input));
    reason = result;
    return { state: WaiterState.SUCCESS, reason };
  } catch (exception) {
    reason = exception;
    if (exception.name === "NotFound") {
      return { state: WaiterState.RETRY, reason };
    }
  }
  return { state: WaiterState.RETRY, reason };
});
var waitForBucketExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  return createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState);
});
var waitUntilBucketExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  const result = yield createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState);
  return checkExceptions(result);
});

// node_modules/@aws-sdk/client-s3/dist-es/waiters/waitForBucketNotExists.js
var checkState2 = (client, input) => __async(void 0, null, function* () {
  let reason;
  try {
    let result = yield client.send(new HeadBucketCommand(input));
    reason = result;
  } catch (exception) {
    reason = exception;
    if (exception.name === "NotFound") {
      return { state: WaiterState.SUCCESS, reason };
    }
  }
  return { state: WaiterState.RETRY, reason };
});
var waitForBucketNotExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  return createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState2);
});
var waitUntilBucketNotExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  const result = yield createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState2);
  return checkExceptions(result);
});

// node_modules/@aws-sdk/client-s3/dist-es/waiters/waitForObjectExists.js
var checkState3 = (client, input) => __async(void 0, null, function* () {
  let reason;
  try {
    let result = yield client.send(new HeadObjectCommand(input));
    reason = result;
    return { state: WaiterState.SUCCESS, reason };
  } catch (exception) {
    reason = exception;
    if (exception.name === "NotFound") {
      return { state: WaiterState.RETRY, reason };
    }
  }
  return { state: WaiterState.RETRY, reason };
});
var waitForObjectExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  return createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState3);
});
var waitUntilObjectExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  const result = yield createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState3);
  return checkExceptions(result);
});

// node_modules/@aws-sdk/client-s3/dist-es/waiters/waitForObjectNotExists.js
var checkState4 = (client, input) => __async(void 0, null, function* () {
  let reason;
  try {
    let result = yield client.send(new HeadObjectCommand(input));
    reason = result;
  } catch (exception) {
    reason = exception;
    if (exception.name === "NotFound") {
      return { state: WaiterState.SUCCESS, reason };
    }
  }
  return { state: WaiterState.RETRY, reason };
});
var waitForObjectNotExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  return createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState4);
});
var waitUntilObjectNotExists = (params, input) => __async(void 0, null, function* () {
  const serviceDefaults = { minDelay: 5, maxDelay: 120 };
  const result = yield createWaiter(__spreadValues(__spreadValues({}, serviceDefaults), params), input, checkState4);
  return checkExceptions(result);
});

// node_modules/@aws-sdk/client-s3/dist-es/S3.js
var commands = {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  CreateBucketCommand,
  CreateBucketMetadataConfigurationCommand,
  CreateBucketMetadataTableConfigurationCommand,
  CreateMultipartUploadCommand,
  CreateSessionCommand,
  DeleteBucketCommand,
  DeleteBucketAnalyticsConfigurationCommand,
  DeleteBucketCorsCommand,
  DeleteBucketEncryptionCommand,
  DeleteBucketIntelligentTieringConfigurationCommand,
  DeleteBucketInventoryConfigurationCommand,
  DeleteBucketLifecycleCommand,
  DeleteBucketMetadataConfigurationCommand,
  DeleteBucketMetadataTableConfigurationCommand,
  DeleteBucketMetricsConfigurationCommand,
  DeleteBucketOwnershipControlsCommand,
  DeleteBucketPolicyCommand,
  DeleteBucketReplicationCommand,
  DeleteBucketTaggingCommand,
  DeleteBucketWebsiteCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  DeleteObjectTaggingCommand,
  DeletePublicAccessBlockCommand,
  GetBucketAbacCommand,
  GetBucketAccelerateConfigurationCommand,
  GetBucketAclCommand,
  GetBucketAnalyticsConfigurationCommand,
  GetBucketCorsCommand,
  GetBucketEncryptionCommand,
  GetBucketIntelligentTieringConfigurationCommand,
  GetBucketInventoryConfigurationCommand,
  GetBucketLifecycleConfigurationCommand,
  GetBucketLocationCommand,
  GetBucketLoggingCommand,
  GetBucketMetadataConfigurationCommand,
  GetBucketMetadataTableConfigurationCommand,
  GetBucketMetricsConfigurationCommand,
  GetBucketNotificationConfigurationCommand,
  GetBucketOwnershipControlsCommand,
  GetBucketPolicyCommand,
  GetBucketPolicyStatusCommand,
  GetBucketReplicationCommand,
  GetBucketRequestPaymentCommand,
  GetBucketTaggingCommand,
  GetBucketVersioningCommand,
  GetBucketWebsiteCommand,
  GetObjectCommand,
  GetObjectAclCommand,
  GetObjectAttributesCommand,
  GetObjectLegalHoldCommand,
  GetObjectLockConfigurationCommand,
  GetObjectRetentionCommand,
  GetObjectTaggingCommand,
  GetObjectTorrentCommand,
  GetPublicAccessBlockCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListBucketAnalyticsConfigurationsCommand,
  ListBucketIntelligentTieringConfigurationsCommand,
  ListBucketInventoryConfigurationsCommand,
  ListBucketMetricsConfigurationsCommand,
  ListBucketsCommand,
  ListDirectoryBucketsCommand,
  ListMultipartUploadsCommand,
  ListObjectsCommand,
  ListObjectsV2Command,
  ListObjectVersionsCommand,
  ListPartsCommand,
  PutBucketAbacCommand,
  PutBucketAccelerateConfigurationCommand,
  PutBucketAclCommand,
  PutBucketAnalyticsConfigurationCommand,
  PutBucketCorsCommand,
  PutBucketEncryptionCommand,
  PutBucketIntelligentTieringConfigurationCommand,
  PutBucketInventoryConfigurationCommand,
  PutBucketLifecycleConfigurationCommand,
  PutBucketLoggingCommand,
  PutBucketMetricsConfigurationCommand,
  PutBucketNotificationConfigurationCommand,
  PutBucketOwnershipControlsCommand,
  PutBucketPolicyCommand,
  PutBucketReplicationCommand,
  PutBucketRequestPaymentCommand,
  PutBucketTaggingCommand,
  PutBucketVersioningCommand,
  PutBucketWebsiteCommand,
  PutObjectCommand,
  PutObjectAclCommand,
  PutObjectLegalHoldCommand,
  PutObjectLockConfigurationCommand,
  PutObjectRetentionCommand,
  PutObjectTaggingCommand,
  PutPublicAccessBlockCommand,
  RenameObjectCommand,
  RestoreObjectCommand,
  SelectObjectContentCommand,
  UpdateBucketMetadataInventoryTableConfigurationCommand,
  UpdateBucketMetadataJournalTableConfigurationCommand,
  UpdateObjectEncryptionCommand,
  UploadPartCommand,
  UploadPartCopyCommand,
  WriteGetObjectResponseCommand
};
var paginators = {
  paginateListBuckets,
  paginateListDirectoryBuckets,
  paginateListObjectsV2,
  paginateListParts
};
var waiters = {
  waitUntilBucketExists,
  waitUntilBucketNotExists,
  waitUntilObjectExists,
  waitUntilObjectNotExists
};
var S3 = class extends S3Client {
};
createAggregatedClient(commands, S3, { paginators, waiters });

// node_modules/@aws-sdk/client-s3/dist-es/models/enums.js
var BucketAbacStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var RequestCharged = {
  requester: "requester"
};
var RequestPayer = {
  requester: "requester"
};
var BucketAccelerateStatus = {
  Enabled: "Enabled",
  Suspended: "Suspended"
};
var Type = {
  AmazonCustomerByEmail: "AmazonCustomerByEmail",
  CanonicalUser: "CanonicalUser",
  Group: "Group"
};
var Permission = {
  FULL_CONTROL: "FULL_CONTROL",
  READ: "READ",
  READ_ACP: "READ_ACP",
  WRITE: "WRITE",
  WRITE_ACP: "WRITE_ACP"
};
var OwnerOverride = {
  Destination: "Destination"
};
var ChecksumType = {
  COMPOSITE: "COMPOSITE",
  FULL_OBJECT: "FULL_OBJECT"
};
var ServerSideEncryption = {
  AES256: "AES256",
  aws_fsx: "aws:fsx",
  aws_kms: "aws:kms",
  aws_kms_dsse: "aws:kms:dsse"
};
var ObjectCannedACL = {
  authenticated_read: "authenticated-read",
  aws_exec_read: "aws-exec-read",
  bucket_owner_full_control: "bucket-owner-full-control",
  bucket_owner_read: "bucket-owner-read",
  private: "private",
  public_read: "public-read",
  public_read_write: "public-read-write"
};
var ChecksumAlgorithm2 = {
  CRC32: "CRC32",
  CRC32C: "CRC32C",
  CRC64NVME: "CRC64NVME",
  MD5: "MD5",
  SHA1: "SHA1",
  SHA256: "SHA256",
  SHA512: "SHA512",
  XXHASH128: "XXHASH128",
  XXHASH3: "XXHASH3",
  XXHASH64: "XXHASH64"
};
var MetadataDirective = {
  COPY: "COPY",
  REPLACE: "REPLACE"
};
var ObjectLockLegalHoldStatus = {
  OFF: "OFF",
  ON: "ON"
};
var ObjectLockMode = {
  COMPLIANCE: "COMPLIANCE",
  GOVERNANCE: "GOVERNANCE"
};
var StorageClass = {
  DEEP_ARCHIVE: "DEEP_ARCHIVE",
  EXPRESS_ONEZONE: "EXPRESS_ONEZONE",
  FSX_ONTAP: "FSX_ONTAP",
  FSX_OPENZFS: "FSX_OPENZFS",
  GLACIER: "GLACIER",
  GLACIER_IR: "GLACIER_IR",
  INTELLIGENT_TIERING: "INTELLIGENT_TIERING",
  ONEZONE_IA: "ONEZONE_IA",
  OUTPOSTS: "OUTPOSTS",
  REDUCED_REDUNDANCY: "REDUCED_REDUNDANCY",
  SNOW: "SNOW",
  STANDARD: "STANDARD",
  STANDARD_IA: "STANDARD_IA"
};
var TaggingDirective = {
  COPY: "COPY",
  REPLACE: "REPLACE"
};
var BucketCannedACL = {
  authenticated_read: "authenticated-read",
  private: "private",
  public_read: "public-read",
  public_read_write: "public-read-write"
};
var BucketNamespace = {
  ACCOUNT_REGIONAL: "account-regional",
  GLOBAL: "global"
};
var DataRedundancy = {
  SingleAvailabilityZone: "SingleAvailabilityZone",
  SingleLocalZone: "SingleLocalZone"
};
var BucketType = {
  Directory: "Directory"
};
var LocationType = {
  AvailabilityZone: "AvailabilityZone",
  LocalZone: "LocalZone"
};
var BucketLocationConstraint = {
  EU: "EU",
  af_south_1: "af-south-1",
  ap_east_1: "ap-east-1",
  ap_east_2: "ap-east-2",
  ap_northeast_1: "ap-northeast-1",
  ap_northeast_2: "ap-northeast-2",
  ap_northeast_3: "ap-northeast-3",
  ap_south_1: "ap-south-1",
  ap_south_2: "ap-south-2",
  ap_southeast_1: "ap-southeast-1",
  ap_southeast_2: "ap-southeast-2",
  ap_southeast_3: "ap-southeast-3",
  ap_southeast_4: "ap-southeast-4",
  ap_southeast_5: "ap-southeast-5",
  ap_southeast_6: "ap-southeast-6",
  ap_southeast_7: "ap-southeast-7",
  ca_central_1: "ca-central-1",
  ca_west_1: "ca-west-1",
  cn_north_1: "cn-north-1",
  cn_northwest_1: "cn-northwest-1",
  eu_central_1: "eu-central-1",
  eu_central_2: "eu-central-2",
  eu_north_1: "eu-north-1",
  eu_south_1: "eu-south-1",
  eu_south_2: "eu-south-2",
  eu_west_1: "eu-west-1",
  eu_west_2: "eu-west-2",
  eu_west_3: "eu-west-3",
  il_central_1: "il-central-1",
  me_central_1: "me-central-1",
  me_south_1: "me-south-1",
  mx_central_1: "mx-central-1",
  sa_east_1: "sa-east-1",
  us_east_2: "us-east-2",
  us_gov_east_1: "us-gov-east-1",
  us_gov_west_1: "us-gov-west-1",
  us_west_1: "us-west-1",
  us_west_2: "us-west-2"
};
var ObjectOwnership = {
  BucketOwnerEnforced: "BucketOwnerEnforced",
  BucketOwnerPreferred: "BucketOwnerPreferred",
  ObjectWriter: "ObjectWriter"
};
var InventoryConfigurationState = {
  DISABLED: "DISABLED",
  ENABLED: "ENABLED"
};
var TableSseAlgorithm = {
  AES256: "AES256",
  aws_kms: "aws:kms"
};
var ExpirationState = {
  DISABLED: "DISABLED",
  ENABLED: "ENABLED"
};
var SessionMode = {
  ReadOnly: "ReadOnly",
  ReadWrite: "ReadWrite"
};
var AnalyticsS3ExportFileFormat = {
  CSV: "CSV"
};
var StorageClassAnalysisSchemaVersion = {
  V_1: "V_1"
};
var EncryptionType = {
  NONE: "NONE",
  SSE_C: "SSE-C"
};
var IntelligentTieringStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var IntelligentTieringAccessTier = {
  ARCHIVE_ACCESS: "ARCHIVE_ACCESS",
  DEEP_ARCHIVE_ACCESS: "DEEP_ARCHIVE_ACCESS"
};
var InventoryFormat = {
  CSV: "CSV",
  ORC: "ORC",
  Parquet: "Parquet"
};
var InventoryIncludedObjectVersions = {
  All: "All",
  Current: "Current"
};
var InventoryOptionalField = {
  BucketKeyStatus: "BucketKeyStatus",
  ChecksumAlgorithm: "ChecksumAlgorithm",
  ETag: "ETag",
  EncryptionStatus: "EncryptionStatus",
  IntelligentTieringAccessTier: "IntelligentTieringAccessTier",
  IsMultipartUploaded: "IsMultipartUploaded",
  LastModifiedDate: "LastModifiedDate",
  LifecycleExpirationDate: "LifecycleExpirationDate",
  ObjectAccessControlList: "ObjectAccessControlList",
  ObjectLockLegalHoldStatus: "ObjectLockLegalHoldStatus",
  ObjectLockMode: "ObjectLockMode",
  ObjectLockRetainUntilDate: "ObjectLockRetainUntilDate",
  ObjectOwner: "ObjectOwner",
  ReplicationStatus: "ReplicationStatus",
  Size: "Size",
  StorageClass: "StorageClass"
};
var InventoryFrequency = {
  Daily: "Daily",
  Weekly: "Weekly"
};
var TransitionStorageClass = {
  DEEP_ARCHIVE: "DEEP_ARCHIVE",
  GLACIER: "GLACIER",
  GLACIER_IR: "GLACIER_IR",
  INTELLIGENT_TIERING: "INTELLIGENT_TIERING",
  ONEZONE_IA: "ONEZONE_IA",
  STANDARD_IA: "STANDARD_IA"
};
var ExpirationStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var TransitionDefaultMinimumObjectSize = {
  all_storage_classes_128K: "all_storage_classes_128K",
  varies_by_storage_class: "varies_by_storage_class"
};
var BucketLogsPermission = {
  FULL_CONTROL: "FULL_CONTROL",
  READ: "READ",
  WRITE: "WRITE"
};
var PartitionDateSource = {
  DeliveryTime: "DeliveryTime",
  EventTime: "EventTime"
};
var S3TablesBucketType = {
  aws: "aws",
  customer: "customer"
};
var Event = {
  s3_IntelligentTiering: "s3:IntelligentTiering",
  s3_LifecycleExpiration_: "s3:LifecycleExpiration:*",
  s3_LifecycleExpiration_Delete: "s3:LifecycleExpiration:Delete",
  s3_LifecycleExpiration_DeleteMarkerCreated: "s3:LifecycleExpiration:DeleteMarkerCreated",
  s3_LifecycleTransition: "s3:LifecycleTransition",
  s3_ObjectAcl_Put: "s3:ObjectAcl:Put",
  s3_ObjectCreated_: "s3:ObjectCreated:*",
  s3_ObjectCreated_CompleteMultipartUpload: "s3:ObjectCreated:CompleteMultipartUpload",
  s3_ObjectCreated_Copy: "s3:ObjectCreated:Copy",
  s3_ObjectCreated_Post: "s3:ObjectCreated:Post",
  s3_ObjectCreated_Put: "s3:ObjectCreated:Put",
  s3_ObjectRemoved_: "s3:ObjectRemoved:*",
  s3_ObjectRemoved_Delete: "s3:ObjectRemoved:Delete",
  s3_ObjectRemoved_DeleteMarkerCreated: "s3:ObjectRemoved:DeleteMarkerCreated",
  s3_ObjectRestore_: "s3:ObjectRestore:*",
  s3_ObjectRestore_Completed: "s3:ObjectRestore:Completed",
  s3_ObjectRestore_Delete: "s3:ObjectRestore:Delete",
  s3_ObjectRestore_Post: "s3:ObjectRestore:Post",
  s3_ObjectTagging_: "s3:ObjectTagging:*",
  s3_ObjectTagging_Delete: "s3:ObjectTagging:Delete",
  s3_ObjectTagging_Put: "s3:ObjectTagging:Put",
  s3_ReducedRedundancyLostObject: "s3:ReducedRedundancyLostObject",
  s3_Replication_: "s3:Replication:*",
  s3_Replication_OperationFailedReplication: "s3:Replication:OperationFailedReplication",
  s3_Replication_OperationMissedThreshold: "s3:Replication:OperationMissedThreshold",
  s3_Replication_OperationNotTracked: "s3:Replication:OperationNotTracked",
  s3_Replication_OperationReplicatedAfterThreshold: "s3:Replication:OperationReplicatedAfterThreshold"
};
var FilterRuleName = {
  prefix: "prefix",
  suffix: "suffix"
};
var DeleteMarkerReplicationStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var MetricsStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var ReplicationTimeStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var ExistingObjectReplicationStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var ReplicaModificationsStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var SseKmsEncryptedObjectsStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var ReplicationRuleStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var Payer = {
  BucketOwner: "BucketOwner",
  Requester: "Requester"
};
var MFADeleteStatus = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var BucketVersioningStatus = {
  Enabled: "Enabled",
  Suspended: "Suspended"
};
var Protocol = {
  http: "http",
  https: "https"
};
var ReplicationStatus = {
  COMPLETE: "COMPLETE",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
  PENDING: "PENDING",
  REPLICA: "REPLICA"
};
var ChecksumMode = {
  ENABLED: "ENABLED"
};
var ObjectAttributes = {
  CHECKSUM: "Checksum",
  ETAG: "ETag",
  OBJECT_PARTS: "ObjectParts",
  OBJECT_SIZE: "ObjectSize",
  STORAGE_CLASS: "StorageClass"
};
var ObjectLockEnabled = {
  Enabled: "Enabled"
};
var ObjectLockRetentionMode = {
  COMPLIANCE: "COMPLIANCE",
  GOVERNANCE: "GOVERNANCE"
};
var ArchiveStatus = {
  ARCHIVE_ACCESS: "ARCHIVE_ACCESS",
  DEEP_ARCHIVE_ACCESS: "DEEP_ARCHIVE_ACCESS"
};
var EncodingType = {
  url: "url"
};
var ObjectStorageClass = {
  DEEP_ARCHIVE: "DEEP_ARCHIVE",
  EXPRESS_ONEZONE: "EXPRESS_ONEZONE",
  FSX_ONTAP: "FSX_ONTAP",
  FSX_OPENZFS: "FSX_OPENZFS",
  GLACIER: "GLACIER",
  GLACIER_IR: "GLACIER_IR",
  INTELLIGENT_TIERING: "INTELLIGENT_TIERING",
  ONEZONE_IA: "ONEZONE_IA",
  OUTPOSTS: "OUTPOSTS",
  REDUCED_REDUNDANCY: "REDUCED_REDUNDANCY",
  SNOW: "SNOW",
  STANDARD: "STANDARD",
  STANDARD_IA: "STANDARD_IA"
};
var OptionalObjectAttributes = {
  RESTORE_STATUS: "RestoreStatus"
};
var ObjectVersionStorageClass = {
  STANDARD: "STANDARD"
};
var MFADelete = {
  Disabled: "Disabled",
  Enabled: "Enabled"
};
var Tier = {
  Bulk: "Bulk",
  Expedited: "Expedited",
  Standard: "Standard"
};
var ExpressionType = {
  SQL: "SQL"
};
var CompressionType = {
  BZIP2: "BZIP2",
  GZIP: "GZIP",
  NONE: "NONE"
};
var FileHeaderInfo = {
  IGNORE: "IGNORE",
  NONE: "NONE",
  USE: "USE"
};
var JSONType = {
  DOCUMENT: "DOCUMENT",
  LINES: "LINES"
};
var QuoteFields = {
  ALWAYS: "ALWAYS",
  ASNEEDED: "ASNEEDED"
};
var RestoreRequestType = {
  SELECT: "SELECT"
};
export {
  Command as $Command,
  AbacStatus$,
  AbortIncompleteMultipartUpload$,
  AbortMultipartUpload$,
  AbortMultipartUploadCommand,
  AbortMultipartUploadOutput$,
  AbortMultipartUploadRequest$,
  AccelerateConfiguration$,
  AccessControlPolicy$,
  AccessControlTranslation$,
  AccessDenied,
  AccessDenied$,
  AnalyticsAndOperator$,
  AnalyticsConfiguration$,
  AnalyticsExportDestination$,
  AnalyticsFilter$,
  AnalyticsS3BucketDestination$,
  AnalyticsS3ExportFileFormat,
  ArchiveStatus,
  BlockedEncryptionTypes$,
  Bucket$,
  BucketAbacStatus,
  BucketAccelerateStatus,
  BucketAlreadyExists,
  BucketAlreadyExists$,
  BucketAlreadyOwnedByYou,
  BucketAlreadyOwnedByYou$,
  BucketCannedACL,
  BucketInfo$,
  BucketLifecycleConfiguration$,
  BucketLocationConstraint,
  BucketLoggingStatus$,
  BucketLogsPermission,
  BucketNamespace,
  BucketType,
  BucketVersioningStatus,
  CORSConfiguration$,
  CORSRule$,
  CSVInput$,
  CSVOutput$,
  Checksum$,
  ChecksumAlgorithm2 as ChecksumAlgorithm,
  ChecksumMode,
  ChecksumType,
  CommonPrefix$,
  CompleteMultipartUpload$,
  CompleteMultipartUploadCommand,
  CompleteMultipartUploadOutput$,
  CompleteMultipartUploadRequest$,
  CompletedMultipartUpload$,
  CompletedPart$,
  CompressionType,
  Condition$,
  ContinuationEvent$,
  CopyObject$,
  CopyObjectCommand,
  CopyObjectOutput$,
  CopyObjectRequest$,
  CopyObjectResult$,
  CopyPartResult$,
  CreateBucket$,
  CreateBucketCommand,
  CreateBucketConfiguration$,
  CreateBucketMetadataConfiguration$,
  CreateBucketMetadataConfigurationCommand,
  CreateBucketMetadataConfigurationRequest$,
  CreateBucketMetadataTableConfiguration$,
  CreateBucketMetadataTableConfigurationCommand,
  CreateBucketMetadataTableConfigurationRequest$,
  CreateBucketOutput$,
  CreateBucketRequest$,
  CreateMultipartUpload$,
  CreateMultipartUploadCommand,
  CreateMultipartUploadOutput$,
  CreateMultipartUploadRequest$,
  CreateSession$,
  CreateSessionCommand,
  CreateSessionOutput$,
  CreateSessionRequest$,
  DataRedundancy,
  DefaultRetention$,
  Delete$,
  DeleteBucket$,
  DeleteBucketAnalyticsConfiguration$,
  DeleteBucketAnalyticsConfigurationCommand,
  DeleteBucketAnalyticsConfigurationRequest$,
  DeleteBucketCommand,
  DeleteBucketCors$,
  DeleteBucketCorsCommand,
  DeleteBucketCorsRequest$,
  DeleteBucketEncryption$,
  DeleteBucketEncryptionCommand,
  DeleteBucketEncryptionRequest$,
  DeleteBucketIntelligentTieringConfiguration$,
  DeleteBucketIntelligentTieringConfigurationCommand,
  DeleteBucketIntelligentTieringConfigurationRequest$,
  DeleteBucketInventoryConfiguration$,
  DeleteBucketInventoryConfigurationCommand,
  DeleteBucketInventoryConfigurationRequest$,
  DeleteBucketLifecycle$,
  DeleteBucketLifecycleCommand,
  DeleteBucketLifecycleRequest$,
  DeleteBucketMetadataConfiguration$,
  DeleteBucketMetadataConfigurationCommand,
  DeleteBucketMetadataConfigurationRequest$,
  DeleteBucketMetadataTableConfiguration$,
  DeleteBucketMetadataTableConfigurationCommand,
  DeleteBucketMetadataTableConfigurationRequest$,
  DeleteBucketMetricsConfiguration$,
  DeleteBucketMetricsConfigurationCommand,
  DeleteBucketMetricsConfigurationRequest$,
  DeleteBucketOwnershipControls$,
  DeleteBucketOwnershipControlsCommand,
  DeleteBucketOwnershipControlsRequest$,
  DeleteBucketPolicy$,
  DeleteBucketPolicyCommand,
  DeleteBucketPolicyRequest$,
  DeleteBucketReplication$,
  DeleteBucketReplicationCommand,
  DeleteBucketReplicationRequest$,
  DeleteBucketRequest$,
  DeleteBucketTagging$,
  DeleteBucketTaggingCommand,
  DeleteBucketTaggingRequest$,
  DeleteBucketWebsite$,
  DeleteBucketWebsiteCommand,
  DeleteBucketWebsiteRequest$,
  DeleteMarkerEntry$,
  DeleteMarkerReplication$,
  DeleteMarkerReplicationStatus,
  DeleteObject$,
  DeleteObjectCommand,
  DeleteObjectOutput$,
  DeleteObjectRequest$,
  DeleteObjectTagging$,
  DeleteObjectTaggingCommand,
  DeleteObjectTaggingOutput$,
  DeleteObjectTaggingRequest$,
  DeleteObjects$,
  DeleteObjectsCommand,
  DeleteObjectsOutput$,
  DeleteObjectsRequest$,
  DeletePublicAccessBlock$,
  DeletePublicAccessBlockCommand,
  DeletePublicAccessBlockRequest$,
  DeletedObject$,
  Destination$,
  DestinationResult$,
  EncodingType,
  Encryption$,
  EncryptionConfiguration$,
  EncryptionType,
  EncryptionTypeMismatch,
  EncryptionTypeMismatch$,
  EndEvent$,
  ErrorDetails$,
  ErrorDocument$,
  Event,
  EventBridgeConfiguration$,
  ExistingObjectReplication$,
  ExistingObjectReplicationStatus,
  ExpirationState,
  ExpirationStatus,
  ExpressionType,
  FileHeaderInfo,
  FilterRule$,
  FilterRuleName,
  GetBucketAbac$,
  GetBucketAbacCommand,
  GetBucketAbacOutput$,
  GetBucketAbacRequest$,
  GetBucketAccelerateConfiguration$,
  GetBucketAccelerateConfigurationCommand,
  GetBucketAccelerateConfigurationOutput$,
  GetBucketAccelerateConfigurationRequest$,
  GetBucketAcl$,
  GetBucketAclCommand,
  GetBucketAclOutput$,
  GetBucketAclRequest$,
  GetBucketAnalyticsConfiguration$,
  GetBucketAnalyticsConfigurationCommand,
  GetBucketAnalyticsConfigurationOutput$,
  GetBucketAnalyticsConfigurationRequest$,
  GetBucketCors$,
  GetBucketCorsCommand,
  GetBucketCorsOutput$,
  GetBucketCorsRequest$,
  GetBucketEncryption$,
  GetBucketEncryptionCommand,
  GetBucketEncryptionOutput$,
  GetBucketEncryptionRequest$,
  GetBucketIntelligentTieringConfiguration$,
  GetBucketIntelligentTieringConfigurationCommand,
  GetBucketIntelligentTieringConfigurationOutput$,
  GetBucketIntelligentTieringConfigurationRequest$,
  GetBucketInventoryConfiguration$,
  GetBucketInventoryConfigurationCommand,
  GetBucketInventoryConfigurationOutput$,
  GetBucketInventoryConfigurationRequest$,
  GetBucketLifecycleConfiguration$,
  GetBucketLifecycleConfigurationCommand,
  GetBucketLifecycleConfigurationOutput$,
  GetBucketLifecycleConfigurationRequest$,
  GetBucketLocation$,
  GetBucketLocationCommand,
  GetBucketLocationOutput$,
  GetBucketLocationRequest$,
  GetBucketLogging$,
  GetBucketLoggingCommand,
  GetBucketLoggingOutput$,
  GetBucketLoggingRequest$,
  GetBucketMetadataConfiguration$,
  GetBucketMetadataConfigurationCommand,
  GetBucketMetadataConfigurationOutput$,
  GetBucketMetadataConfigurationRequest$,
  GetBucketMetadataConfigurationResult$,
  GetBucketMetadataTableConfiguration$,
  GetBucketMetadataTableConfigurationCommand,
  GetBucketMetadataTableConfigurationOutput$,
  GetBucketMetadataTableConfigurationRequest$,
  GetBucketMetadataTableConfigurationResult$,
  GetBucketMetricsConfiguration$,
  GetBucketMetricsConfigurationCommand,
  GetBucketMetricsConfigurationOutput$,
  GetBucketMetricsConfigurationRequest$,
  GetBucketNotificationConfiguration$,
  GetBucketNotificationConfigurationCommand,
  GetBucketNotificationConfigurationRequest$,
  GetBucketOwnershipControls$,
  GetBucketOwnershipControlsCommand,
  GetBucketOwnershipControlsOutput$,
  GetBucketOwnershipControlsRequest$,
  GetBucketPolicy$,
  GetBucketPolicyCommand,
  GetBucketPolicyOutput$,
  GetBucketPolicyRequest$,
  GetBucketPolicyStatus$,
  GetBucketPolicyStatusCommand,
  GetBucketPolicyStatusOutput$,
  GetBucketPolicyStatusRequest$,
  GetBucketReplication$,
  GetBucketReplicationCommand,
  GetBucketReplicationOutput$,
  GetBucketReplicationRequest$,
  GetBucketRequestPayment$,
  GetBucketRequestPaymentCommand,
  GetBucketRequestPaymentOutput$,
  GetBucketRequestPaymentRequest$,
  GetBucketTagging$,
  GetBucketTaggingCommand,
  GetBucketTaggingOutput$,
  GetBucketTaggingRequest$,
  GetBucketVersioning$,
  GetBucketVersioningCommand,
  GetBucketVersioningOutput$,
  GetBucketVersioningRequest$,
  GetBucketWebsite$,
  GetBucketWebsiteCommand,
  GetBucketWebsiteOutput$,
  GetBucketWebsiteRequest$,
  GetObject$,
  GetObjectAcl$,
  GetObjectAclCommand,
  GetObjectAclOutput$,
  GetObjectAclRequest$,
  GetObjectAttributes$,
  GetObjectAttributesCommand,
  GetObjectAttributesOutput$,
  GetObjectAttributesParts$,
  GetObjectAttributesRequest$,
  GetObjectCommand,
  GetObjectLegalHold$,
  GetObjectLegalHoldCommand,
  GetObjectLegalHoldOutput$,
  GetObjectLegalHoldRequest$,
  GetObjectLockConfiguration$,
  GetObjectLockConfigurationCommand,
  GetObjectLockConfigurationOutput$,
  GetObjectLockConfigurationRequest$,
  GetObjectOutput$,
  GetObjectRequest$,
  GetObjectRetention$,
  GetObjectRetentionCommand,
  GetObjectRetentionOutput$,
  GetObjectRetentionRequest$,
  GetObjectTagging$,
  GetObjectTaggingCommand,
  GetObjectTaggingOutput$,
  GetObjectTaggingRequest$,
  GetObjectTorrent$,
  GetObjectTorrentCommand,
  GetObjectTorrentOutput$,
  GetObjectTorrentRequest$,
  GetPublicAccessBlock$,
  GetPublicAccessBlockCommand,
  GetPublicAccessBlockOutput$,
  GetPublicAccessBlockRequest$,
  GlacierJobParameters$,
  Grant$,
  Grantee$,
  HeadBucket$,
  HeadBucketCommand,
  HeadBucketOutput$,
  HeadBucketRequest$,
  HeadObject$,
  HeadObjectCommand,
  HeadObjectOutput$,
  HeadObjectRequest$,
  IdempotencyParameterMismatch,
  IdempotencyParameterMismatch$,
  IndexDocument$,
  Initiator$,
  InputSerialization$,
  IntelligentTieringAccessTier,
  IntelligentTieringAndOperator$,
  IntelligentTieringConfiguration$,
  IntelligentTieringFilter$,
  IntelligentTieringStatus,
  InvalidObjectState,
  InvalidObjectState$,
  InvalidRequest,
  InvalidRequest$,
  InvalidWriteOffset,
  InvalidWriteOffset$,
  InventoryConfiguration$,
  InventoryConfigurationState,
  InventoryDestination$,
  InventoryEncryption$,
  InventoryFilter$,
  InventoryFormat,
  InventoryFrequency,
  InventoryIncludedObjectVersions,
  InventoryOptionalField,
  InventoryS3BucketDestination$,
  InventorySchedule$,
  InventoryTableConfiguration$,
  InventoryTableConfigurationResult$,
  InventoryTableConfigurationUpdates$,
  JSONInput$,
  JSONOutput$,
  JSONType,
  JournalTableConfiguration$,
  JournalTableConfigurationResult$,
  JournalTableConfigurationUpdates$,
  LambdaFunctionConfiguration$,
  LifecycleExpiration$,
  LifecycleRule$,
  LifecycleRuleAndOperator$,
  LifecycleRuleFilter$,
  ListBucketAnalyticsConfigurations$,
  ListBucketAnalyticsConfigurationsCommand,
  ListBucketAnalyticsConfigurationsOutput$,
  ListBucketAnalyticsConfigurationsRequest$,
  ListBucketIntelligentTieringConfigurations$,
  ListBucketIntelligentTieringConfigurationsCommand,
  ListBucketIntelligentTieringConfigurationsOutput$,
  ListBucketIntelligentTieringConfigurationsRequest$,
  ListBucketInventoryConfigurations$,
  ListBucketInventoryConfigurationsCommand,
  ListBucketInventoryConfigurationsOutput$,
  ListBucketInventoryConfigurationsRequest$,
  ListBucketMetricsConfigurations$,
  ListBucketMetricsConfigurationsCommand,
  ListBucketMetricsConfigurationsOutput$,
  ListBucketMetricsConfigurationsRequest$,
  ListBuckets$,
  ListBucketsCommand,
  ListBucketsOutput$,
  ListBucketsRequest$,
  ListDirectoryBuckets$,
  ListDirectoryBucketsCommand,
  ListDirectoryBucketsOutput$,
  ListDirectoryBucketsRequest$,
  ListMultipartUploads$,
  ListMultipartUploadsCommand,
  ListMultipartUploadsOutput$,
  ListMultipartUploadsRequest$,
  ListObjectVersions$,
  ListObjectVersionsCommand,
  ListObjectVersionsOutput$,
  ListObjectVersionsRequest$,
  ListObjects$,
  ListObjectsCommand,
  ListObjectsOutput$,
  ListObjectsRequest$,
  ListObjectsV2$,
  ListObjectsV2Command,
  ListObjectsV2Output$,
  ListObjectsV2Request$,
  ListParts$,
  ListPartsCommand,
  ListPartsOutput$,
  ListPartsRequest$,
  LocationInfo$,
  LocationType,
  LoggingEnabled$,
  MFADelete,
  MFADeleteStatus,
  MetadataConfiguration$,
  MetadataConfigurationResult$,
  MetadataDirective,
  MetadataEntry$,
  MetadataTableConfiguration$,
  MetadataTableConfigurationResult$,
  MetadataTableEncryptionConfiguration$,
  Metrics$,
  MetricsAndOperator$,
  MetricsConfiguration$,
  MetricsFilter$,
  MetricsStatus,
  MultipartUpload$,
  NoSuchBucket,
  NoSuchBucket$,
  NoSuchKey,
  NoSuchKey$,
  NoSuchUpload,
  NoSuchUpload$,
  NoncurrentVersionExpiration$,
  NoncurrentVersionTransition$,
  NotFound,
  NotFound$,
  NotificationConfiguration$,
  NotificationConfigurationFilter$,
  ObjectAlreadyInActiveTierError,
  ObjectAlreadyInActiveTierError$,
  ObjectAttributes,
  ObjectCannedACL,
  ObjectEncryption$,
  ObjectIdentifier$,
  ObjectLockConfiguration$,
  ObjectLockEnabled,
  ObjectLockLegalHold$,
  ObjectLockLegalHoldStatus,
  ObjectLockMode,
  ObjectLockRetention$,
  ObjectLockRetentionMode,
  ObjectLockRule$,
  ObjectNotInActiveTierError,
  ObjectNotInActiveTierError$,
  ObjectOwnership,
  ObjectPart$,
  ObjectStorageClass,
  ObjectVersion$,
  ObjectVersionStorageClass,
  OptionalObjectAttributes,
  OutputLocation$,
  OutputSerialization$,
  Owner$,
  OwnerOverride,
  OwnershipControls$,
  OwnershipControlsRule$,
  ParquetInput$,
  Part$,
  PartitionDateSource,
  PartitionedPrefix$,
  Payer,
  Permission,
  PolicyStatus$,
  Progress$,
  ProgressEvent$,
  Protocol,
  PublicAccessBlockConfiguration$,
  PutBucketAbac$,
  PutBucketAbacCommand,
  PutBucketAbacRequest$,
  PutBucketAccelerateConfiguration$,
  PutBucketAccelerateConfigurationCommand,
  PutBucketAccelerateConfigurationRequest$,
  PutBucketAcl$,
  PutBucketAclCommand,
  PutBucketAclRequest$,
  PutBucketAnalyticsConfiguration$,
  PutBucketAnalyticsConfigurationCommand,
  PutBucketAnalyticsConfigurationRequest$,
  PutBucketCors$,
  PutBucketCorsCommand,
  PutBucketCorsRequest$,
  PutBucketEncryption$,
  PutBucketEncryptionCommand,
  PutBucketEncryptionRequest$,
  PutBucketIntelligentTieringConfiguration$,
  PutBucketIntelligentTieringConfigurationCommand,
  PutBucketIntelligentTieringConfigurationRequest$,
  PutBucketInventoryConfiguration$,
  PutBucketInventoryConfigurationCommand,
  PutBucketInventoryConfigurationRequest$,
  PutBucketLifecycleConfiguration$,
  PutBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationOutput$,
  PutBucketLifecycleConfigurationRequest$,
  PutBucketLogging$,
  PutBucketLoggingCommand,
  PutBucketLoggingRequest$,
  PutBucketMetricsConfiguration$,
  PutBucketMetricsConfigurationCommand,
  PutBucketMetricsConfigurationRequest$,
  PutBucketNotificationConfiguration$,
  PutBucketNotificationConfigurationCommand,
  PutBucketNotificationConfigurationRequest$,
  PutBucketOwnershipControls$,
  PutBucketOwnershipControlsCommand,
  PutBucketOwnershipControlsRequest$,
  PutBucketPolicy$,
  PutBucketPolicyCommand,
  PutBucketPolicyRequest$,
  PutBucketReplication$,
  PutBucketReplicationCommand,
  PutBucketReplicationRequest$,
  PutBucketRequestPayment$,
  PutBucketRequestPaymentCommand,
  PutBucketRequestPaymentRequest$,
  PutBucketTagging$,
  PutBucketTaggingCommand,
  PutBucketTaggingRequest$,
  PutBucketVersioning$,
  PutBucketVersioningCommand,
  PutBucketVersioningRequest$,
  PutBucketWebsite$,
  PutBucketWebsiteCommand,
  PutBucketWebsiteRequest$,
  PutObject$,
  PutObjectAcl$,
  PutObjectAclCommand,
  PutObjectAclOutput$,
  PutObjectAclRequest$,
  PutObjectCommand,
  PutObjectLegalHold$,
  PutObjectLegalHoldCommand,
  PutObjectLegalHoldOutput$,
  PutObjectLegalHoldRequest$,
  PutObjectLockConfiguration$,
  PutObjectLockConfigurationCommand,
  PutObjectLockConfigurationOutput$,
  PutObjectLockConfigurationRequest$,
  PutObjectOutput$,
  PutObjectRequest$,
  PutObjectRetention$,
  PutObjectRetentionCommand,
  PutObjectRetentionOutput$,
  PutObjectRetentionRequest$,
  PutObjectTagging$,
  PutObjectTaggingCommand,
  PutObjectTaggingOutput$,
  PutObjectTaggingRequest$,
  PutPublicAccessBlock$,
  PutPublicAccessBlockCommand,
  PutPublicAccessBlockRequest$,
  QueueConfiguration$,
  QuoteFields,
  RecordExpiration$,
  RecordsEvent$,
  Redirect$,
  RedirectAllRequestsTo$,
  RenameObject$,
  RenameObjectCommand,
  RenameObjectOutput$,
  RenameObjectRequest$,
  ReplicaModifications$,
  ReplicaModificationsStatus,
  ReplicationConfiguration$,
  ReplicationRule$,
  ReplicationRuleAndOperator$,
  ReplicationRuleFilter$,
  ReplicationRuleStatus,
  ReplicationStatus,
  ReplicationTime$,
  ReplicationTimeStatus,
  ReplicationTimeValue$,
  RequestCharged,
  RequestPayer,
  RequestPaymentConfiguration$,
  RequestProgress$,
  RestoreObject$,
  RestoreObjectCommand,
  RestoreObjectOutput$,
  RestoreObjectRequest$,
  RestoreRequest$,
  RestoreRequestType,
  RestoreStatus$,
  RoutingRule$,
  S3,
  S3Client,
  S3KeyFilter$,
  S3Location$,
  S3ServiceException,
  S3ServiceException$,
  S3TablesBucketType,
  S3TablesDestination$,
  S3TablesDestinationResult$,
  SSEKMS$,
  SSEKMSEncryption$,
  SSES3$,
  ScanRange$,
  SelectObjectContent$,
  SelectObjectContentCommand,
  SelectObjectContentEventStream$,
  SelectObjectContentOutput$,
  SelectObjectContentRequest$,
  SelectParameters$,
  ServerSideEncryption,
  ServerSideEncryptionByDefault$,
  ServerSideEncryptionConfiguration$,
  ServerSideEncryptionRule$,
  SessionCredentials$,
  SessionMode,
  SimplePrefix$,
  SourceSelectionCriteria$,
  SseKmsEncryptedObjects$,
  SseKmsEncryptedObjectsStatus,
  Stats$,
  StatsEvent$,
  StorageClass,
  StorageClassAnalysis$,
  StorageClassAnalysisDataExport$,
  StorageClassAnalysisSchemaVersion,
  TableSseAlgorithm,
  Tag$,
  Tagging$,
  TaggingDirective,
  TargetGrant$,
  TargetObjectKeyFormat$,
  Tier,
  Tiering$,
  TooManyParts,
  TooManyParts$,
  TopicConfiguration$,
  Transition$,
  TransitionDefaultMinimumObjectSize,
  TransitionStorageClass,
  Type,
  UpdateBucketMetadataInventoryTableConfiguration$,
  UpdateBucketMetadataInventoryTableConfigurationCommand,
  UpdateBucketMetadataInventoryTableConfigurationRequest$,
  UpdateBucketMetadataJournalTableConfiguration$,
  UpdateBucketMetadataJournalTableConfigurationCommand,
  UpdateBucketMetadataJournalTableConfigurationRequest$,
  UpdateObjectEncryption$,
  UpdateObjectEncryptionCommand,
  UpdateObjectEncryptionRequest$,
  UpdateObjectEncryptionResponse$,
  UploadPart$,
  UploadPartCommand,
  UploadPartCopy$,
  UploadPartCopyCommand,
  UploadPartCopyOutput$,
  UploadPartCopyRequest$,
  UploadPartOutput$,
  UploadPartRequest$,
  VersioningConfiguration$,
  WebsiteConfiguration$,
  WriteGetObjectResponse$,
  WriteGetObjectResponseCommand,
  WriteGetObjectResponseRequest$,
  _Error$,
  _Object$,
  Client as __Client,
  errorTypeRegistries,
  paginateListBuckets,
  paginateListDirectoryBuckets,
  paginateListObjectsV2,
  paginateListParts,
  waitForBucketExists,
  waitForBucketNotExists,
  waitForObjectExists,
  waitForObjectNotExists,
  waitUntilBucketExists,
  waitUntilBucketNotExists,
  waitUntilObjectExists,
  waitUntilObjectNotExists
};
//# sourceMappingURL=@aws-sdk_client-s3.js.map
