/*
** Module: rivm-portaal-data
**
** scheduled proces to get ApriSensor data and post it to RIVM portaal
**
**

voor testen resultaat door op te vragen met de volgende API:
curl -i -XPOST 'http://influx.rivm.nl:8086/read?db=db_ApriSensor' -u <user>:'<password>'
// --data-binary 'm_abc,id=sensor01 lat=52.345,lon=4.567,PM=13.4,T=21.5,timestamp_from="2017-12-31T23:05:00Z",timestamp_to="2017-12-31T23:06:00Z"'

*/
// **********************************************************************************
"use strict"; // This is for your code to comply with the ECMAScript 5 standard.

var request = require('request');
var fs 		= require('fs');
var sys 	= require('util');
var Influx = require('influx');

var _options	= {};
var apriSensorUrl
var secureSite;
var siteProtocol;
var openIoDUrl;
var fiwareService;
var fiwareServicePath;
var influx;

var sensorIds=[]
var sensorIdsIndex=-1
var sensorIdParams={}

var startDateTimeFilter, endDateTimeFilter

// **********************************************************************************

var processSensorIds = function(){
	sensorIdsIndex+=1

	if (sensorIdsIndex>sensorIds.length-1) {
		return
	}

	if (sensorIds[sensorIdsIndex].active=='FALSE') {
		processSensorIds()
		return
	}
	processSensorId()
}

var processSensorId= function () {
	sensorIdParams = {};

	sensorIdParams = sensorIds[sensorIdsIndex]
	sensorIdParams.key = 'sensorId';
	sensorIdParams.opPerRow = 'true';

	var tmpOps = sensorIdParams.ops.split(',')
	sensorIdParams.observableProperties	= []
	for (var i=0;i< tmpOps.length;i++) {
		var observableProperty		= {}
		if(tmpOps=='pm25'){
			observableProperty.id	= 'pm25';
			observableProperty.uom	= 'µg/m3';
			observableProperty.externalName	= 'PM2.5';
			observableProperty.sensorType	= 'PMSA003';
			sensorIdParams.observableProperties.push(observableProperty);
		}
		if(tmpOps=='pm10'){
			observableProperty.id	= 'pm10';
			observableProperty.uom	= 'µg/m3';
			observableProperty.externalName	= 'PM10';
			observableProperty.sensorType	= 'PMSA003';
			sensorIdParams.observableProperties.push(observableProperty);
		}
		if(tmpOps=='scapeler_dylos_raw0'){
			observableProperty.id	= 'scapeler_dylos_raw0';
			observableProperty.uom	= 'p/0.01 cb.ft.';
			observableProperty.externalName	= 'PM2.5';
			observableProperty.sensorType	= 'Dylos';
			sensorIdParams.observableProperties.push(observableProperty);
		}
		if(tmpOps=='scapeler_dylos_raw1'){
			observableProperty.id	= 'scapeler_dylos_raw1';
			observableProperty.uom	= 'p/0.01 cb.ft.';
			observableProperty.externalName	= 'PM10';
			observableProperty.sensorType	= 'Dylos';
			sensorIdParams.observableProperties.push(observableProperty);
		}
	}

	sensorIdParams.dateFrom	= startDateTimeFilter.toISOString();
	sensorIdParams.dateTo		= endDateTimeFilter.toISOString();
	sensorIdParams.format = 'json';
	getOpenIoD();
}

var getOpenIoD = function(){
	var _url = openIoDUrl +
		"?fiwareService=" +sensorIdParams.fiwareService+
		"&fiwareServicePath=" +sensorIdParams.fiwareServicePath+
		"&key="+sensorIdParams.key+
		"&opPerRow="+sensorIdParams.opPerRow+
		"&foiOps="+sensorIdParams.sensorId+","+sensorIdParams.ops+
		'&dateFrom=' + sensorIdParams.dateFrom +
		'&dateTo=' + sensorIdParams.dateTo+
		'&format=' + sensorIdParams.format
		;
	console.log(_url);

	let body = [];
	request.get(_url)
	.on('response', function(response) {
		console.log(response.statusCode) // 200
	})
	.on('data', (chunk) => {
		body.push(chunk);
	})
	.on('end', () => {
		body = Buffer.concat(body).toString();
		processData(body);
	})
	.on('error', function(err) {
		console.log(err)
		processSensorIds()
	})
}

var processData = function(data) {
	var _data = JSON.parse(data);
	if (_data.length==0) {
		console.log('No data found for '+sensorIdParams.sensorId)
		processSensorIds()
		return
	}

	var measurement = {};
	measurement.measurement	= _options.configParameter.rivmPortalDatabaseMeasurement;
	measurement.tags= [];
	measurement.fields= {};

	var totals={'count':0}

	for (var i=0;i<_data.length;i++) {
		var observation = _data[i];

		if (observation.scapeler_dylos_raw0!=undefined) {
			if (totals.scapeler_dylos_raw0 ==undefined) totals.scapeler_dylos_raw0=0
			totals.scapeler_dylos_raw0+=observation.scapeler_dylos_raw0
		}
		if (observation.scapeler_dylos_raw1!=undefined) {
			if (totals.scapeler_dylos_raw1 ==undefined) totals.scapeler_dylos_raw1=0
			totals.scapeler_dylos_raw1+=observation.scapeler_dylos_raw1
		}
		if (observation.pm25!=undefined) {
			if (totals.pm25 ==undefined) totals.pm25=0
			totals.pm25+=observation.pm25
		}
		if (observation.pm10!=undefined) {
			if (totals.pm10 ==undefined) totals.pm10=0
			totals.pm10+=observation.pm10
		}
		totals.count+=1

		if(i==0) {
			measurement.fields.timestamp_from = observation.dateObserved
			measurement.fields.timestamp_to = observation.dateObserved
		} else {
			if (measurement.fields.timestamp_from > observation.dateObserved) {
				measurement.fields.timestamp_from = observation.dateObserved
			}
			if (measurement.fields.timestamp_to < observation.dateObserved) {
				measurement.fields.timestamp_to = observation.dateObserved
			}
		}
	}  // end of for loop observations in _data

	if (totals.count==0) {
		console.log('No data found for '+sensorIdParams.sensorId)
		processSensorIds()
		return
	}

	measurement.tags.id 					= sensorIdParams.externalId
	measurement.fields.lat 				= sensorIdParams.lat
	measurement.fields.lon 				= sensorIdParams.lon
	measurement.fields["PM"+"-meetopstelling"]	= sensorIdParams.sensorType;  // Dylos DC1700

	if (totals.scapeler_dylos_raw0 != undefined && totals.scapeler_dylos_raw1 != undefined) {
		var dylos	= {};  // special for dylos to calculate PM10 PM2.5 ug/m3
		dylos.pm25UgM3	= ((totals.scapeler_dylos_raw0 - totals.scapeler_dylos_raw1 )/totals.count) / 250;
		dylos.pm10UgM3	= dylos.pm25UgM3 * 1.43;
		dylos.pm25UgM3	= Math.round(dylos.pm25UgM3*100)/100+0.5;
		dylos.pm10UgM3	= Math.round(dylos.pm10UgM3*100)/100+0.5;
		console.log(totals.scapeler_dylos_raw0/totals.count + '->' + dylos.pm25UgM3 +
		 	' & ' + totals.scapeler_dylos_raw1/totals.count + '->' + dylos.pm10UgM3 );
		measurement.fields['PM2.5'] = dylos.pm25UgM3;
		measurement.fields['PM10'] = dylos.pm10UgM3;
		measurement.fields['PM'] = dylos.pm10UgM3;
		measurement.fields['PM-meetopstelling']= 'Dylos';
	}
	if (totals.pm25 != undefined && totals.pm10 != undefined) {
		console.log(totals.pm25/totals.count + ' & ' + totals.pm10/totals.count)
		measurement.fields['PM2.5'] = Math.round((totals.pm25/totals.count)*100)/100
		measurement.fields['PM10'] = Math.round((totals.pm10/totals.count)*100)/100
		measurement.fields['PM'] = Math.round((totals.pm10/totals.count)*100)/100
		measurement.fields['PM-meetopstelling']= sensorIdParams.sensorType
	}

	console.dir(measurement);

	influx.writePoints([
		measurement
	])
	.then(result=>{
		console.log('influx then')
		processSensorIds()
	})
	.catch(err => {
		console.log(err)
		processSensorIds()
		return;
	})

/*
	console.log('Toon laatste records in Influx database:')

	influx.query('select * from '+_options.configParameter.rivmPortalDatabaseMeasurement+' order by time desc limit 2')
	.then(result => {
		console.log(result);
	})
	.catch(err => {
		console.log(err)
		return;
	})
*/
}

module.exports = {
	init: function (options) {
		_options					= options;

		secureSite 			= true;
		siteProtocol 		= secureSite==true?'https://':'http://';
		openIoDUrl			= siteProtocol + 'aprisensor-in.openiod.org/apri-sensor-service/v1/getSelectionData/';

		influx = new Influx.InfluxDB({
			host: options.configParameter.rivmPortalDatabaseHost,
			port: options.configParameter.rivmPortalDatabasePort,
			database: options.configParameter.rivmPortalDatabaseName,
			username: options.configParameter.rivmPortalDatabaseAccount,
			password: options.configParameter.rivmPortalDatabasePassword,
			schema: [{
				measurement: options.configParameter.rivmPortalDatabaseMeasurement,
				fields: {
					lat: Influx.FieldType.FLOAT,
					lon: Influx.FieldType.FLOAT,
					timestamp_from: Influx.FieldType.STRING,
					timestamp_to: Influx.FieldType.STRING,
					"PM2.5":Influx.FieldType.FLOAT,
					"PM2.5-eenheid": Influx.FieldType.STRING,
					"PM2.5-meetopstelling": Influx.FieldType.STRING,
					"PM10":Influx.FieldType.FLOAT,
					"PM10-eenheid": Influx.FieldType.STRING,
					"PM10-meetopstelling": Influx.FieldType.STRING,
					"PM":Influx.FieldType.FLOAT,
					"PM-eenheid": Influx.FieldType.STRING,
					"PM-meetopstelling": Influx.FieldType.STRING,
				},
				tags: [
					'id'
				]
			}]
		})

		// Retrieve measurements time window from 2 hours ago and 1 hour upward
		// scheduled execution per hour
		var _dateTime = new Date(new Date().getTime() - 7200000);
		_dateTime = new Date(_dateTime.getTime()
				- (_dateTime.getSeconds()*1000)
				- _dateTime.getMilliseconds())
		startDateTimeFilter 	= new Date(_dateTime.getTime());
		endDateTimeFilter		= new Date(_dateTime.getTime()+3600000-1);
		console.log(startDateTimeFilter);
		console.log(endDateTimeFilter);

//		console.dir(_options);

//		if (options.argvStations == undefined) {
//			console.log('Parameter with archivedate is missing, processing with default (actual-2h) date.');
//		}
		var sensorIdsIn = fs.readFileSync('../config/sensorIdsRivm.json','utf8')
		sensorIds = JSON.parse(sensorIdsIn)
		processSensorIds();
	}
}
