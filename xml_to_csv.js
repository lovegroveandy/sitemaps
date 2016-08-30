var LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader('./potato.xml');
var fs = require('fs'), wstream = fs.createWriteStream('./test_new.csv')

var list = []
lr.on('error', function (err) {
	// 'err' contains error object
});

lr.on('line', function (line) {
  line = line.replace("<url>\n", "<url>").replace("</loc>\n", "</loc>").replace("</lastmod>\n", "</lastmod>").replace("</url>", "</url>\n").replace("2016-08-16", "2016-08-26")
  list.push(line)
	// 'line' contains the current line without the trailing newline character.
});

lr.on('end', function () {
	// All lines are read, file is closed now.
  list = list.join("")
  wstream.write(list)
});
