'use strict';
exports.handler = async (event, context, callback) => {
    // get the response
    const response = event.Records[0].cf.response;
    const headers = response.headers;
    const request = event.Records[0].cf.request;
    const uri = request.uri.split("/")
    const filename = uri[uri.length - 1]
    const splitname = filename.split(".")
    const suffix = splitname[splitname.length - 1]
    // Add our CORS and other headers always
    headers['access-control-allow-origin'] = [{key: 'access-control-allow-origin', value: '*'}];
    headers['access-control-allow-methods'] = [{key: 'access-control-allow-methods', value: '*'}];
    headers['access-control-allow-headers'] = [{key: 'access-control-allow-headers', value: 'range'}];
    headers['access-control-expose-headers'] = [{key: 'access-control-expose-headers', value: 'content-length,content-range'}];
    // Assign content-type based on file suffix
    if (suffix == "m3u8") {
        headers['content-type'] = [{key: 'content-type', value: 'application/vnd.apple.mpegurl'}];
        headers['cache-control'] = [{key: 'cache-control', value: 'max-age=3'}]
    } else if (suffix == "ts") {
        headers['content-type'] = [{key: 'content-type', value: 'video/MP2T'}]
    } else if (suffix == "vtt") {
        headers['content-type'] = [{key: 'content-type', value: 'text/vtt'}]
    }
    // continue with the response
    return callback(null, response);
};