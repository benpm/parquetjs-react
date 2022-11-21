'use strict';
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types')


/** We need to use a patched version of TFramedTransport where
 * readString returns the original buffer instead of a string if the
 * buffer can not be safely encoded as utf8 (see http://bit.ly/2GXeZEF)
 */

class fixedTFramedTransport extends thrift.TFramedTransport {
  readString(len) {
    this.ensureAvailable(len);
    var buffer = this.inBuf.slice(this.readPos, this.readPos + len);
    var str = this.inBuf.toString('utf8', this.readPos, this.readPos + len);
    this.readPos += len;
    return (Buffer.from(str).equals(buffer)) ? str : buffer;
  }
}


/** Patch PageLocation to be three element array that has getters/setters
 * for each of the properties (offset, compressed_page_size, first_row_index)
 * This saves space considerably as we do not need to store the full variable
 * names for every PageLocation
 */

const previousPageLocation = parquet_thrift.PageLocation.prototype;
const PageLocation = parquet_thrift.PageLocation.prototype = [];
PageLocation.write = previousPageLocation.write;
PageLocation.read = previousPageLocation.read;

const getterSetter = index => ({
  get: function () { return this[index]; },
  set: function (value) { return this[index] = value; }
});

Object.defineProperty(PageLocation, 'offset', getterSetter(0));
Object.defineProperty(PageLocation, 'compressed_page_size', getterSetter(1));
Object.defineProperty(PageLocation, 'first_row_index', getterSetter(2));


exports.force32 = function () {
  const protocol = thrift.TCompactProtocol.prototype;
  protocol.zigzagToI64 = protocol.zigzagToI32;
  protocol.readVarint64 = protocol.readVarint32 = function () {
    let lo = 0;
    let shift = 0;
    let b;
    while (true) {
      b = this.trans.readByte();
      lo = lo | ((b & 0x7f) << shift);
      shift += 7;
      if (!(b & 0x80)) {
        break;
      }
    }
    return lo;
  };
}

/**
 * Helper function that serializes a thrift object into a buffer
 */
exports.serializeThrift = function (obj) {
  let output = []

  let transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })

  let protocol = new thrift.TCompactProtocol(transport)
  obj.write(protocol)
  transport.flush()

  return Buffer.concat(output)
}

exports.decodeThrift = function (obj, buf, offset) {
  if (!offset) {
    offset = 0;
  }

  var transport = new fixedTFramedTransport(buf);
  transport.readPos = offset;
  var protocol = new thrift.TCompactProtocol(transport);
  obj.read(protocol);
  return transport.readPos - offset;
}

/**
 * Get the number of bits required to store a given value
 */
exports.getBitWidth = function (val) {
  if (val === 0) {
    return 0;
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
}

/**
 * FIXME not ideal that this is linear
 */
exports.getThriftEnum = function (klass, value) {
  for (let k in klass) {
    if (klass[k] === value) {
      return k;
    }
  }

  throw 'Invalid ENUM value';
}

exports.fopen = function (filePath) {
  throw 'UNIMPLEMENTED';
}

exports.fstat = function (filePath) {
  throw 'UNIMPLEMENTED';
}

exports.fread = function (fd, position, length) {
  throw 'UNIMPLEMENTED';
}

exports.fclose = function (fd) {
  throw 'UNIMPLEMENTED';
}

exports.oswrite = function (os, buf) {
  throw 'UNIMPLEMENTED';
}

exports.osend = function (os) {
  throw 'UNIMPLEMENTED';
}

exports.osopen = function (path, opts) {
  throw 'UNIMPLEMENTED';
}

exports.fieldIndexOf = function (arr, elem) {
  for (let j = 0; j < arr.length; ++j) {
    if (arr[j].length !== elem.length) {
      continue;
    }

    let m = true;
    for (let i = 0; i < elem.length; ++i) {
      if (arr[j][i] !== elem[i]) {
        m = false;
        break;
      }
    }

    if (m) {
      return j;
    }
  }

  return -1;
}