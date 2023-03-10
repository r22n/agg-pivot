const path = require('path');

module.exports = {
  entry: './cdn.js',
  output: {
    filename: 'index.min.js',
    path: path.resolve(__dirname, 'dist'),
  },
};