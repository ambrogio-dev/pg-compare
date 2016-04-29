global.verbose

var log = {}

/**
 * output management
 * @function
 * @param {...} args any arguments
 */
log.verbose = function () {
  var _args = Array.prototype.slice.call(arguments)
  var _mode = _args.shift()
  // / if mode is error write it in error log, else write it in log only if verbose is true
  if (_mode === 'error') {
    console.error.apply(console, _args)
  } else if (global.verbose) {
    console.log.apply(console, _args)
  }
}

log.setVerbose = function (verbose) {
  global.verbose = verbose
}

module.exports = log
