const path = require("path");

module.exports = {
    entry: "./static/js/index.js",
    output: {
	path: path.resolve(__dirname, "static/dist"),
	filename: "bundle.js"
    },
    module: {
	rules: [
	    {
		test: /\.s[ac]ss$/i,
		use: [
		    // Creates `style` nodes from JS strings
		    'style-loader',
		    // Translates CSS into CommonJS
		    'css-loader',
		    // Compiles Sass to CSS
		    'sass-loader',
		],
	    },
	    {
		test: /\.css$/i,
		use: ['style-loader', 'css-loader'],
	    }
	]
    }
}
