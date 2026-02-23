const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');

module.exports = {
  entry: './module.ts',
  output: {
    filename: 'module.js',
    path: path.resolve(__dirname, 'dist'),
    libraryTarget: 'amd',
    publicPath: '',
  },
  externals: [
    'lodash',
    'jquery',
    'moment',
    'react',
    'react-dom',
    'react-redux',
    'redux',
    'rxjs',
    /^@grafana\/.*/,
    /^@emotion\/.*/,
  ],
  resolve: {
    extensions: ['.ts', '.tsx', '.js'],
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: {
          loader: 'ts-loader',
          options: { transpileOnly: true },
        },
        exclude: /node_modules/,
      },
    ],
  },
  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        { from: 'plugin.json', to: '.' },
        { from: 'img', to: 'img' },
      ],
    }),
  ],
};
