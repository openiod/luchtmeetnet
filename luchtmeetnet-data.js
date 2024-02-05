/*
** Module: luchtmeetnet-data
**
**
**
**
*/
// **********************************************************************************
"use strict"; // This is for your code to comply with the ECMAScript 5 standard.

var request = require('request');
var fs = require('fs');
var sys = require('util');
var _options = {};
var luchtmeetnetUrl, luchtmeetnetFileName, luchtmeetnetLocalPathRoot, fileFolder, tmpFolder;
var secureSite;
var siteProtocol;
var openiodUrl;

let PUBLIC_API_VERSION = "v1"
let PUBLIC_PRODUCTION = "false"
let PUBLIC_ORIGIN = "http://localhost:5174"
let PUBLIC_APRISENSOR_URL_PROD = "http://localhost:3000"
let PUBLIC_APRISENSOR_URL_TESTxx = "http://localhost:5174"
let PUBLIC_APRISENSOR_URL_TESTx = "https://aprisensor-api-v1.openiod.org:3100"
let PUBLIC_APRISENSOR_URL_TEST = "https://aprisensor-api-v1.openiod.org"
let ORIGIN = "https://dataportaal.openiod.org:3000"


var sensorIds = []
var sensorIdsIndex

var processSensors = function () {
	sensorIdsIndex += 1
	if (sensorIdsIndex < sensorIds.length) {
		console.log('Processing sensorId: ' + sensorIds[sensorIdsIndex]);
		reqFile(luchtmeetnetUrl + sensorIds[sensorIdsIndex] + '/measurements'
			, luchtmeetnetFileName, false, 'luchtmeetnetdata', sensorIds[sensorIdsIndex]);
	}
}

var reqFile = function (url, fileName, unzip, desc, sensorId) {
	var _wfsResult = null;
	console.log("Request start: " + desc + " (" + url + ")");

	async function StreamBuffer(req) {
		var self = this
		var buffer = []
		var ended = false
		var ondata = null
		var onend = null

		self.ondata = function (f) {
			console.log("self.ondata")
			for (var i = 0; i < buffer.length; i++) {
				f(buffer[i])
			}
			ondata = f
		}

		self.onend = function (f) {
			onend = f
			if (ended) {
				onend()
			}
		}

		req.on('data', function (chunk) {
			if (_wfsResult) {
				_wfsResult += chunk;
			} else {
				_wfsResult = chunk;
			}

			if (ondata) {
				ondata(chunk)
			} else {
				buffer.push(chunk)
			}
		})

		req.on('end', function () {
			ended = true;
			if (onend) {
				onend()
			}
		})
		req.streambuffer = self
	}

	function writeFile(path, fileName, content) {
		fs.writeFile(path + fileName, content, function (err) {
			if (err) {
				console.log(err);
			} else {
				console.log("The file is saved! " + tmpFolder + fileName + ' (unzip:' + unzip + ')');
				if (unzip) {
					var exec = require('child_process').exec;
					var puts = function (error, stdout, stderr) { sys.puts(stdout) }
					exec(" cd " + tmpFolder + " ;  unzip -o " + tmpFolder + fileName + " ", puts);
				}
			}
		});
	}


	var sendApriSensorData2 = async function (data) {

		var urlEndpoint = '';
		if (PUBLIC_PRODUCTION == 'false') {
			urlEndpoint = PUBLIC_APRISENSOR_URL_TEST;
		} else {
			urlEndpoint = PUBLIC_APRISENSOR_URL_PROD;
		}
		urlEndpoint = urlEndpoint + '/' + PUBLIC_API_VERSION + '/luchtmeetnet'

		var init = {
			method: 'POST',
			headers: {
				accept: 'application/json',
				'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
			},
			//		body: JSON.stringify(formBodyStr)
			body: JSON.stringify(data)
		};

		console.log(data)

		await fetch(urlEndpoint, init)
			.then(function (response) {
				console.log(response.status)
			})
			.catch(function (error) {
				if (error.response) {
					console.log(error.response)
					//					console.log(error.response.statusTekst)
					//					console.log(error.response.data)
				} else {
					console.log(error)
				}
			})
	}


	var sendData = function (data) {
		// oud //		http://openiod.com/SCAPE604/openiod?SERVICE=WPS&REQUEST=Execute&identifier=transform_observation&inputformat=insertom&objectid=humansensor&format=xml
		// oud //			&region=EHV		&lat=50.1		&lng=4.0		&category=airquality		&value=1
		//http://localhost:4000/SCAPE604/openiod?SERVICE=WPS&REQUEST=Execute&identifier=transform_observation&action=insertom&sensorsystem=scapeler_shinyei&offering=offering_0439_initial&verbose=true&commit=true&observation=scapeler_shinyei:12.345&neighborhoodcode=BU04390402
		//https://openiod.org/SCAPE604/openiod?SERVICE=WPS&REQUEST=Execute&identifier=transform_observation&action=insertom&sensorsystem=scapeler_shinyei&offering=offering_0439_initial&verbose=true&commit=true&observation=scapeler_shinyei:12.345&neighborhoodcode=BU04390402

		var _url = openiodUrl + '/openiod?SERVICE=WPS&REQUEST=Execute&identifier=transform_observation&action=insertom&sensorsystem=apri-sensor-luchtmeetnet&offering=offering_0439_initial&commit=true';
		_url = _url + '&foi=' + data.foi + '&observation=' + data.observation + '&measurementTime=' + data.measurementTime.toISOString();
		console.log(_url);
		request.get(_url)
			.on('response', function (response) {
				console.log(response.statusCode) // 200
				processSensors()
			})
			.on('error', function (err) {
				console.log(err)
				processSensors()
			})
			;
	};

	var options = {
		uri: url,
		method: 'GET'
	};

	request(options, function (error, response, body) {

		if (error) {
			processSensors()
			return;
		}
		if (response.statusCode != 200) {
			processSensors()
			return;
		}
		var inRecordJson = JSON.parse(body);
		var inRecord = inRecordJson.data;
		if (inRecord.length == 0) {
			console.log('No Luchtmeetnet sensordata found for this url: ' + options.uri);
			processSensors()
			return;
		}

		var data = {};
		var dataDB = {};
		var tmpMeasurements = {};
		dataDB.sensorId = 'LUCHTMEETNET' + sensorId
		dataDB.sensorType = 'lml'
		dataDB.observation = {}
		dataDB.sensorId = 'LUCHTMEETNET' + sensorId
		dataDB.sensorType = 'lml'


		//var i = inRecord.length - 1;  // only last retrieved measurement

		for (var i = 0; i < inRecord.length; i++) {
			var inMeasurement = inRecord[i];
			var measurementTime = new Date(inMeasurement.timestamp_measured);
			var nowTime = new Date();
			var timeDiff = new Date().getTime() - measurementTime.getTime();

			if (timeDiff > 2 * 3600000) {  // only last hour measurements + hour?
				//				console.log('ID: '+ sensorId + ' '+ nowTime + ' measurementtime: ' + measurementTime + ' ignore message timediff > 1 hour' );
				continue; // ignore measurement
			}

			data.measurementTime = new Date(inMeasurement.timestamp_measured);

			dataDB.observation.dateObserved = new Date(inMeasurement.timestamp_measured).toISOString()
			dataDB.observation.dateReceived = new Date().toISOString()

			if (tmpMeasurements[sensorId] == undefined) tmpMeasurements[sensorId] = {};
			var _measurement = tmpMeasurements[sensorId];
			//	_measurement.sensorType = inMeasurement.formula;
			//  console.dir(inMeasurement);
			if (inMeasurement.formula == 'PM25') {
				_measurement.PM25 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'PM10') {
				_measurement.PM10 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'NO') {
				_measurement.NO = inMeasurement.value;
			}
			if (inMeasurement.formula == 'NO2') {
				_measurement.NO2 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'SO2') {
				_measurement.SO2 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'O3') {
				_measurement.O3 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'C6H6') {
				_measurement.C6H6 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'C7H8') {
				_measurement.C7H8 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'C8H10') {
				_measurement.C8H10 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'CO') {
				_measurement.CO = inMeasurement.value;
			}
			if (inMeasurement.formula == 'H2S') {
				_measurement.H2S = inMeasurement.value;
			}
			if (inMeasurement.formula == 'PS') {
				_measurement.PS = inMeasurement.value;
			}
			if (inMeasurement.formula == 'NH3') {
				_measurement.NH3 = inMeasurement.value;
			}
			if (inMeasurement.formula == 'FN') {
				_measurement.FN = inMeasurement.value;
			}
			if (inMeasurement.formula == 'Offset') {
				_measurement.Offset = inMeasurement.value;
			}
		}

		if (_measurement == undefined) {
			console.log('Geen geldige measurement gevonden voor ' + sensorId);
			processSensors()
			return;
		}

		data.foi = 'LUCHTMEETNET' + sensorId;

		data.observation = "";
		if (_measurement.PM25) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-PM25:' + _measurement.PM25;
		}
		if (_measurement.PM10) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-PM10:' + _measurement.PM10;
		}
		if (_measurement.NO) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-NO:' + _measurement.NO;
		}
		if (_measurement.NO2) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-NO2:' + _measurement.NO2;
		}
		if (_measurement.SO2) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-SO2:' + _measurement.SO2;
		}
		if (_measurement.O3) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-O3:' + _measurement.O3;
		}
		if (_measurement.C6H6) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-C6H6:' + _measurement.C6H6;
		}
		if (_measurement.C7H8) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-C7H8:' + _measurement.C7H8;
		}
		if (_measurement.C8H10) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-C8H10:' + _measurement.C8H10;
		}
		if (_measurement.CO) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-CO:' + _measurement.CO;
		}
		if (_measurement.H2S) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-H2S:' + _measurement.H2S;
		}
		if (_measurement.PS) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-PS:' + _measurement.PS;
		}
		if (_measurement.NH3) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-NH3:' + _measurement.NH3;
		}
		if (_measurement.FN) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-FN:' + _measurement.FN;
		}
		if (_measurement.Offset) {
			if (data.observation != "") data.observation = data.observation + ",";
			data.observation = data.observation + 'apri-sensor-luchtmeetnet-Offset:' + _measurement.Offset;
		}


		if (_measurement.PM25) {
			dataDB.observation.pm25 = _measurement.PM25;
		}
		if (_measurement.PM10) {
			dataDB.observation.pm10 = _measurement.PM10;
		}
		if (_measurement.NO) {
			dataDB.observation.no = _measurement.NO;
		}
		if (_measurement.NO2) {
			dataDB.observation.no2 = _measurement.NO2;
		}
		if (_measurement.SO2) {
			dataDB.observation.so2 = _measurement.SO2;
		}
		if (_measurement.O3) {
			dataDB.observation.o3 = _measurement.O3;
		}
		if (_measurement.C6H6) {
			dataDB.observation.c6h6 = _measurement.C6H6;
		}
		if (_measurement.C7H8) {
			dataDB.observation.c7h8 = _measurement.C7H8;
		}
		if (_measurement.C8H10) {
			dataDB.observation.c8h10 = _measurement.C8H10;
		}
		if (_measurement.CO) {
			dataDB.observation.co = _measurement.CO;
		}
		if (_measurement.H2S) {
			dataDB.observation.h2s = _measurement.H2S;
		}
		if (_measurement.PS) {
			dataDB.observation.ps = _measurement.PS;
		}
		if (_measurement.NH3) {
			dataDB.observation.nh3 = _measurement.NH3;
		}
		if (_measurement.FN) {
			dataDB.observation.fn = _measurement.FN;
		}
		if (_measurement.Offset) {
			dataDB.observation.Offset = _measurement.Offset;
		}

		if (data.observation != "") {
			//sendData(data);  // deze kan straks uit
		}
		if (dataDB != {}) {
			sendApriSensorData2(dataDB)
			processSensors()
		} else processSensors()

	});
}


// **********************************************************************************


module.exports = {

	//1238: Zwolle

	init: function (options) {
		_options = options;

		secureSite = true;
		siteProtocol = secureSite ? 'https://' : 'http://';
		openiodUrl = siteProtocol + 'openiod.org/' + _options.systemCode; //SCAPE604';
		//loopTimeMax			= 60000; //ms, 60000=60 sec

		luchtmeetnetUrl = 'https://api.luchtmeetnet.nl/open_api/stations/';
		luchtmeetnetFileName = 'luchtmeetnet.txt';

		luchtmeetnetLocalPathRoot = options.systemFolderParent + '/luchtmeetnet/';
		fileFolder = 'luchtmeetnet';
		tmpFolder = luchtmeetnetLocalPathRoot + fileFolder + "/" + 'tmp/';

		// create subfolders
		try { fs.mkdirSync(tmpFolder); } catch (e) { };//console.log('ERROR: no tmp folder found, batch run aborted.'); return } ;

		//console.dir(_options);

		if (options.argvStations == undefined) {
			console.log('Parameter with sensorId(s) is missing, processing aborted.');
			return;
		}

		sensorIds = _options.argvStations.split(',')
		sensorIdsIndex = -1
		console.log(sensorIds);
		processSensors();
	}
}  // end of module.exports
