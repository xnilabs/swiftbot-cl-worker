// Web Logging Worker with Cloud Pub/Sub Integration
// For public web applications - no authentication required
export default {
  async fetch(request, env, ctx) {
    return await handleRequest(request, env, ctx);
  }
};

// Configuration and Constants
const MAX_BODY_SIZE = 10000; // Max body size to log (bytes)
const ENABLE_LOGGING = true;
const LOG_REQUEST_BODY = true; // Set false to disable logging of request payload
const LOG_RESPONSE_BODY = true; // Set false to disable logging of response payload

async function handleRequest(request, env, ctx) {
  const startTime = Date.now();
  const traceId = crypto.randomUUID(); 
  const url = new URL(request.url);
  
  // Get configuration from environment variables
  const { UPSTREAM_URL } = env;

  try {
    // 1. Validate Environment Variables
    if (!UPSTREAM_URL) {
      const errorLog = createErrorLog(
        request,
        traceId,
        Date.now() - startTime,
        new Error('Missing required environment variable: UPSTREAM_URL'),
        'CONFIGURATION_ERROR'
      );
      ctx.waitUntil(sendLogToPubSub(errorLog, env));

      return new Response(JSON.stringify({
        error: 'Configuration error',
        message: 'Missing required environment variables',
        traceId: traceId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', "X-Request-ID": traceId }
      });
    }

    // 2. Handle all HTTP methods - Forward to upstream
    return await forwardToUpstream(request, url, UPSTREAM_URL, traceId, startTime, ctx, env);

  } catch (error) {
    // Global error handler
    console.error('Unhandled error:', error);
    
    const errorLog = createErrorLog(
      request,
      traceId,
      Date.now() - startTime,
      error,
      'UNHANDLED_ERROR'
    );
    ctx.waitUntil(sendLogToPubSub(errorLog, env));
    
    return new Response(JSON.stringify({ 
      error: 'Internal server error',
      traceId: traceId
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        'X-Request-ID': traceId
      }
    });    
  }
}

/**
 * Capture request body to ArrayBuffer and a loggable object
 */
async function captureAndCloneRequestBody(request) {
    let bodyBuffer = null;
    let bodyContent = null;
    let bodySize = 0;
    const contentType = request.headers.get('content-type') || '';
    
    try {
        const clonedRequest = request.clone();
        bodyBuffer = await clonedRequest.arrayBuffer();
        bodySize = bodyBuffer.byteLength;
        
        if (LOG_REQUEST_BODY && bodySize > 0) {
            const bodyText = new TextDecoder().decode(bodyBuffer);
            
            if (contentType.includes('application/json')) {
                try {
                    bodyContent = JSON.parse(bodyText);
                } catch (e) {
                    bodyContent = bodyText; // Not valid JSON
                }
            } else if (contentType.includes('application/x-www-form-urlencoded')) {
                // Parse form data
                const params = new URLSearchParams(bodyText);
                bodyContent = Object.fromEntries(params);
            } else {
                bodyContent = bodyText; // Non-JSON content
            }
            
            if (bodySize > MAX_BODY_SIZE) {
                const preview = (typeof bodyContent === 'string' ? bodyContent : JSON.stringify(bodyContent)).substring(0, MAX_BODY_SIZE);

                bodyContent = {
                    _truncated: true,
                    _original_size: bodySize,
                    _preview: preview + '...'
                };
            }
        } else if (bodySize > 0) {
             bodyContent = { _logging_disabled: !LOG_REQUEST_BODY, _size: bodySize };
        }
        
    } catch (e) {
        bodyContent = { _error: 'Failed to capture body', _message: e.message };
        bodyBuffer = null; 
    }
    
    return { bodyBuffer, bodyContent, bodySize };
}

// Forward requests to upstream server
async function forwardToUpstream(request, url, UPSTREAM_URL, traceId, startTime, ctx, env) {
  
  try {
    // 1. Capture Request Body (for POST, PUT, PATCH)
    let bodyBuffer = null;
    let bodyContent = null;
    let bodySize = 0;
    
    if (['POST', 'PUT', 'PATCH'].includes(request.method)) {
      const captured = await captureAndCloneRequestBody(request);
      bodyBuffer = captured.bodyBuffer;
      bodyContent = captured.bodyContent;
      bodySize = captured.bodySize;
    }

    // 2. Prepare Upstream Request
    const targetUrl = new URL(UPSTREAM_URL);
    // Append original path and query params
    targetUrl.pathname = url.pathname;
    targetUrl.search = url.search;
    
    const newHeaders = new Headers();
    newHeaders.set("Host", targetUrl.hostname);
    newHeaders.set("X-Request-ID", traceId);
    newHeaders.set("X-Forwarded-For", request.headers.get('cf-connecting-ip') || 'unknown');
    newHeaders.set("X-Forwarded-Proto", url.protocol.replace(':', ''));
    newHeaders.set("X-Original-URL", request.url);
    newHeaders.set("X-Real-IP", request.headers.get('cf-connecting-ip') || 'unknown');
    
    // Forward all headers from original request
    const headersToForward = [
      'content-type', 
      'accept', 
      'accept-encoding', 
      'accept-language',
      'user-agent',
      'referer',
      'origin',
      'authorization',
      'cookie',
      'cache-control',
      'pragma'
    ];
    
    const forwardedHeaders = {};
    for (const header of headersToForward) {
      const value = request.headers.get(header);
      if (value) {
        newHeaders.set(header, value);
        // Mask sensitive headers in log
        if (['authorization', 'cookie'].includes(header.toLowerCase())) {
          forwardedHeaders[header] = '***';
        } else {
          forwardedHeaders[header] = value;
        }
      }
    }
    
    // 3. Call Upstream and wait for response
    let upstreamResp = null;
    let responseStatus = 0;
    let responseBodyParsed = null;
    let responseBodyText = '';
    let upstreamError = null;

    try {
      // Call upstream server
      upstreamResp = await fetch(targetUrl.toString(), {
        method: request.method,
        headers: newHeaders,
        body: bodyBuffer,
        redirect: "follow"
      });
      
      responseStatus = upstreamResp.status;
      
      // Capture response body
      responseBodyText = await upstreamResp.text();
      
      if (LOG_RESPONSE_BODY) {
        responseBodyParsed = await captureResponseData(upstreamResp, responseBodyText);
      } else {
        responseBodyParsed = {
          _logging_disabled: true,
          status: responseStatus,
          size: new Blob([responseBodyText]).size
        };
      }

    } catch (error) {
      console.error('Error calling upstream:', error.message);
      upstreamError = error;
      responseStatus = 502; // Bad Gateway
    }
    
    // 4. Log the request
    if (upstreamError) {
      // Log error
      const errorLog = createErrorLog(
        request,
        traceId,
        Date.now() - startTime,
        upstreamError,
        'UPSTREAM_ERROR',
        responseStatus,
        {
          upstream_url: targetUrl.toString(),
          request_method: request.method,
          request_body: bodyContent,
          body_size: bodySize,
          forwarded_headers: forwardedHeaders,
        }
      );
      ctx.waitUntil(sendLogToPubSub(errorLog, env));
      
      // Return error response
      return new Response(JSON.stringify({
        error: 'Bad Gateway',
        message: 'Failed to reach upstream server',
        traceId: traceId
      }), {
        status: 502,
        headers: {
          'Content-Type': 'application/json',
          'X-Request-ID': traceId
        }
      });
      
    } else {
      // Log successful request
      const requestLog = createRequestLog(
        request,
        traceId,
        Date.now() - startTime,
        'REQUEST_COMPLETED',
        'Request successfully processed',
        responseStatus,
        {
          upstream_url: targetUrl.toString(),
          request_method: request.method,
          request_body: bodyContent,
          body_size: bodySize,
          forwarded_headers: forwardedHeaders,
          upstream_response: responseBodyParsed,
        }
      );
      ctx.waitUntil(sendLogToPubSub(requestLog, env));
      
      // 5. Return upstream response to client
      const responseHeaders = new Headers(upstreamResp.headers);
      responseHeaders.set('X-Request-ID', traceId);
      responseHeaders.set('X-Upstream-Status', responseStatus.toString());
      
      return new Response(responseBodyText, {
        status: responseStatus,
        headers: responseHeaders
      });
    }
    
  } catch (error) {
    console.error('Error processing request:', error);
    
    const errorLog = createErrorLog(
      request,
      traceId,
      Date.now() - startTime,
      error,
      'PROCESSING_ERROR'
    );
    ctx.waitUntil(sendLogToPubSub(errorLog, env));    

    return new Response(JSON.stringify({ 
      error: 'Processing error',
      message: error.message,
      traceId: traceId 
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json', 
        'X-Request-ID': traceId 
      }
    });
  }
}

/**
 * Capture response body for logging
 */
async function captureResponseData(response, bodyText) {
    let bodyParsed = bodyText;
    const bodySize = new Blob([bodyText]).size;
    const contentType = response.headers.get('content-type') || '';
    
    if (bodySize > 0) {
        // Try to parse as JSON
        if (contentType.includes('application/json')) {
            try {
                bodyParsed = JSON.parse(bodyText);
            } catch (e) {
                // Failed to parse, keep as string
            }
        }
        
        // Truncate if too large
        if (bodySize > MAX_BODY_SIZE) {
            const preview = (typeof bodyParsed === 'string' ? bodyParsed : JSON.stringify(bodyParsed)).substring(0, MAX_BODY_SIZE);
            bodyParsed = {
                _truncated: true,
                _original_size: bodySize,
                _preview: preview + '...'
            };
        }
    }

    return {
        status: response.status,
        status_text: response.statusText,
        headers: Object.fromEntries(response.headers),
        body: bodyParsed,
        body_size: bodySize,
    };
}

/**
 * Create log for web requests
 */
function createRequestLog(request, traceId, duration, eventType, message, statusCode, additionalData) {
  const url = new URL(request.url);
  return {
    timestamp: new Date().toISOString(),
    traceId: traceId, 
    type: 'web_request',
    severity: determineSeverity(statusCode),
    
    event: {
      type: eventType,
      message: message
    },
    
    request: {
      method: request.method,
      url: request.url,
      path: url.pathname,
      query_params: Object.fromEntries(url.searchParams),
      headers: sanitizeHeaders(Object.fromEntries(request.headers)),
      protocol: url.protocol,
      host: url.host,
    },
    
    response: {
      status: statusCode,
    },
    
    client: getClientInfo(request),
    
    performance: {
      duration_ms: duration,
      timestamp_start: new Date(Date.now() - duration).toISOString(),
      timestamp_end: new Date().toISOString(),
    },
    
    data: additionalData || {}
  };
}

/**
 * Create log for errors (5xx or exceptions)
 */
function createErrorLog(request, traceId, duration, error, errorType, upstreamStatus = 500, additionalData = {}) {
  const url = new URL(request.url);
  return {
    timestamp: new Date().toISOString(),
    traceId: traceId,
    type: 'system_error',
    severity: 'ERROR',
    
    error: {
      type: errorType,
      message: error.message,
      stack: error.stack,
      name: error.name,
    },
    
    request: {
      method: request.method,
      url: request.url,
      path: url.pathname,
      query_params: Object.fromEntries(url.searchParams),
      headers: sanitizeHeaders(Object.fromEntries(request.headers)),
    },
    
    response: {
        status: upstreamStatus
    },

    client: getClientInfo(request),
    
    performance: {
      duration_ms: duration,
      timestamp_start: new Date(Date.now() - duration).toISOString(),
      timestamp_end: new Date().toISOString(),
    },
    
    data: additionalData
  };
}

/**
 * Sanitize headers - mask sensitive information
 */
function sanitizeHeaders(headers) {
  const sensitiveHeaders = ['authorization', 'cookie', 'x-api-key', 'api-key'];
  const sanitized = { ...headers };
  
  for (const key in sanitized) {
    if (sensitiveHeaders.includes(key.toLowerCase()) && sanitized[key]) {
      sanitized[key] = '***';
    }
  }
  
  return sanitized;
}

/**
 * Get client/Cloudflare information
 */
function getClientInfo(request) {
    return {
        ip: request.headers.get('cf-connecting-ip') || 'unknown',
        user_agent: request.headers.get('user-agent') || 'unknown',
        referer: request.headers.get('referer') || 'unknown',
        origin: request.headers.get('origin') || 'unknown',
        cloudflare: {
            colo: request.cf?.colo || 'unknown',
            country: request.cf?.country || 'unknown',
            city: request.cf?.city || 'unknown',
            region: request.cf?.region || 'unknown',
            asn: request.cf?.asn || 'unknown',
            timezone: request.cf?.timezone || 'unknown',
            tls_version: request.cf?.tlsVersion || 'unknown',
            request_priority: request.cf?.requestPriority || 'unknown',
            http_protocol: request.cf?.httpProtocol || 'unknown'
        }
    };
}

function determineSeverity(status) {
  if (status >= 500) return 'ERROR';
  if (status >= 400) return 'WARNING';
  if (status >= 300) return 'NOTICE';
  return 'INFO';
}

/**
 * Send log to GCP Pub/Sub
 */
async function sendLogToPubSub(logData, env) {
  if (!ENABLE_LOGGING) return;
  
  try {
    if (!env.GCP_SERVICE_ACCOUNT_KEY || !env.GCP_PROJECT_ID || !env.PUBSUB_TOPIC) {
      console.error('Missing GCP configuration for logging');
      return;
    }
    
    const serviceAccount = JSON.parse(env.GCP_SERVICE_ACCOUNT_KEY);
    const token = await getGCPAccessToken(serviceAccount);
    const messageData = btoa(JSON.stringify(logData));
    
    const pubsubUrl = `https://pubsub.googleapis.com/v1/projects/${env.GCP_PROJECT_ID}/topics/${env.PUBSUB_TOPIC}:publish`;
    
    const response = await fetch(pubsubUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        messages: [{
          data: messageData,
          attributes: {
            source: 'cloudflare-web-logger',
            type: logData.type,
            severity: logData.severity,
            request_id: logData.traceId,
            method: logData.request?.method || 'unknown',
            path: logData.request?.path || 'unknown'
          }
        }]
      })
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error('Pub/Sub publish failed:', errorText);
    } else {
      const result = await response.json();
      console.log('Published to Pub/Sub:', result.messageIds?.[0]);
    }
    
  } catch (error) {
    console.error('Error sending to Pub/Sub:', error.message);
  }
}

/**
 * Generate GCP OAuth 2.0 access token from service account
 */
async function getGCPAccessToken(serviceAccount) {
  const header = {
    alg: 'RS256',
    typ: 'JWT'
  };
  
  const now = Math.floor(Date.now() / 1000);
  const claimSet = {
    iss: serviceAccount.client_email,
    scope: 'https://www.googleapis.com/auth/pubsub',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now
  };
  
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedClaim = base64UrlEncode(JSON.stringify(claimSet));
  const signatureInput = `${encodedHeader}.${encodedClaim}`;
  
  const privateKey = await crypto.subtle.importKey(
    'pkcs8',
    pemToArrayBuffer(serviceAccount.private_key),
    {
      name: 'RSASSA-PKCS1-v1_5',
      hash: 'SHA-256'
    },
    false,
    ['sign']
  );
  
  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    privateKey,
    new TextEncoder().encode(signatureInput)
  );
  
  const encodedSignature = base64UrlEncode(signature);
  const jwt = `${signatureInput}.${encodedSignature}`;
  
  const tokenResponse = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt
    })
  });
  
  if (!tokenResponse.ok) {
    throw new Error('Failed to get access token');
  }
  
  const tokenData = await tokenResponse.json();
  return tokenData.access_token;
}

function base64UrlEncode(data) {
  if (typeof data === 'string') {
    data = new TextEncoder().encode(data);
  }
  if (data instanceof ArrayBuffer) {
    data = new Uint8Array(data);
  }
  
  let base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

function pemToArrayBuffer(pem) {
  const b64 = pem
    .replace(/-----BEGIN PRIVATE KEY-----/, '')
    .replace(/-----END PRIVATE KEY-----/, '')
    .replace(/\s/g, '');
  
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes.buffer;
}
