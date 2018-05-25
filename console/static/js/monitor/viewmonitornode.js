//设置时间
$(function(){
	datePairs($('#start'), $('#end'));
});
function formatDate(formatDate){
	  function padding(str){
	    if(str > 0 && str < 10){
	      return '0' + str;
	    }else{
	      return str;
	    }
	  }
	  var year = formatDate.getFullYear(), 
	      month = padding(formatDate.getMonth() + 1), 
	      date = padding(formatDate.getDate()),
	      hour = padding(formatDate.getHours()),
	      minute = padding(formatDate.getMinutes());
	  return year + '-' + month + '-' + date + ' ' + hour + ':' + minute; 
	}
function datePairs(date1, date2, start, maxDate){
	  var nowdate = new Date(),
	      startDate = new Date(nowdate.getTime() - (maxDate || 1000 * 60 * 60 *24 * 30)),
	      defaultEnd = formatDate(nowdate),
	      startVal = start || 4 * 1000 * 60 * 60,
	      defaultStart = formatDate(new Date(nowdate.getTime() - startVal));
	  date1.datetimepicker({
	    format: 'yyyy-mm-dd hh:ii',
	    autoclose: true,
	    todayBtn: true,
	    startDate: startDate,
	    endDate: nowdate
	  })
	  .on('changeDate', function(ev){
	    if (date1.val() > date2.val()){
	        alert("开始时间不能大于结束时间");
	        date1.val(date2.val());
	        date1.datetimepicker('update');
	    }
	  });
	  date2.datetimepicker({
	    format: 'yyyy-mm-dd hh:ii',
	    autoclose: true,
	    todayBtn: true,
	    startDate: startDate,
	    endDate: nowdate
	  }).on('changeDate', function(ev){
	    if (date1.val() > date2.val()){
	        alert("束时间不能小于开始时间");
	        date2.val(date1.val());
	        date2.datetimepicker('update');
	    }
	  });
	  date1.val(defaultStart).datetimepicker('update');
	  date2.val(defaultEnd).datetimepicker('update');
}
var options_load = {
        chart: {
        	renderTo: 'container_load',
            type: 'spline'            
        },
        title: {
            text: '服务器负载监控'
        },
        xAxis: {
        	type: 'datetime',
        	dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            }
        },
        yAxis: {
            title: {
                text: '系统负载'
            },
            labels: {//刻度
                formatter: function() {
                    return this.value;
                },
                style: {
                    color: '#89A54E'
                }
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e. %b}: {point.y:.2f} load'
        },
        plotOptions: {
        	series: {
        		turboThreshold: 0
            },
            spline: {
                marker: {
                    enabled: true
                }
            }
        },
        series: []            
    };

var options_outper_inper = {
		chart: {
        	renderTo: 'container_outper',
            type: 'spline'            
        },
		title: {
            text: '服务器出流量监控'
        },
        xAxis: {
        	type: 'datetime',
        	dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            }
        },
        yAxis: {
        	title: {
                text: '出流量 '
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e. %b}: {point.y:.2f} 流量'
        },
        plotOptions: {
        	series: {
        		turboThreshold: 0
            },
            spline: {
                marker: {
                    enabled: true
                }
            }
        },
        series: []            
};
var options_cpu_rate = {
		chart: {
			renderTo: 'container_cpu_rate',
            text: 'cpu使用率'
        },
		title: {
            text: 'cpu使用率'
        },
        xAxis: {
        	type: 'datetime',
        	dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            }
        },
        yAxis: {
        	title: {
                text: 'cpu使用率 '
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e. %b}: {point.y:.2f} % 使用率'
        },
        plotOptions: {
        	series: {
        		turboThreshold: 0
            },
            spline: {
                marker: {
                    enabled: true
                }
            }
        },
        series: []   
};
var options_memory = {
		chart: {
			renderTo: 'container_memory',
            text: '服务器内存使用情况'
        },
		title: {
            text: '服务器内存使用情况'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{ // visualize the weekend
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_disk = {
		chart: {
			renderTo: 'container_disk',
            text: '服务器磁盘使用情况'
        },
		title: {
            text: '服务器磁盘使用情况'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{ // visualize the weekend
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_swap = {
		chart: {
			renderTo: 'container_swap',
            text: '服务器swap使用情况'
        },
		title: {
            text: '服务器swap使用情况'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{ // visualize the weekend
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_cpu_percent = {
		chart: {
			renderTo: 'container_cpu_percent',
            text: 'cpu使用率'
        },
		title: {
            text: 'cpu使用率'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '单位 %'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' %'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_memory_percent = {
		chart: {
			renderTo: 'container_memory_percent',
            text: '内存使用率'
        },
		title: {
            text: '内存使用率'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '单位 %'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' %'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_memory_swap = {
		chart: {
			renderTo: 'container_memory_swap',
            text: '虚拟内存使用情况'
        },
		title: {
            text: '虚拟内存使用情况'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_io_read_bytes = {
		chart: {
			renderTo: 'container_io_read_bytes',
            text: '读写流量'
        },
		title: {
            text: '读写流量'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};

var options_io_read_count = {
		chart: {
			renderTo: 'container_io_read_count',
            text: '读写计数'
        },
		title: {
            text: '读写计数'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间数量'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Number'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_net_byte_recv = {
		chart: {
			renderTo: 'container_net_byte_recv',
            text: '出入流量统计'
        },
		title: {
            text: '出入流量统计'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_net_connection_count = {
		chart: {
			renderTo: 'container_net_connection_count',
            text: '连接计数'
        },
		title: {
            text: '连接计数'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 数量'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' number'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_net_drop_in = {
		chart: {
			renderTo: 'container_net_drop_in',
            text: '丢包统计'
        },
		title: {
            text: '丢包统计'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_net_err_in = {
		chart: {
			renderTo: 'container_net_err_in',
            text: '网络错误统计'
        },
		title: {
            text: '网络错误统计'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_net_packet_recv = {
		chart: {
			renderTo: 'container_net_packet_recv',
            text: '收发包量'
        },
		title: {
            text: '收发包量'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 数量'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Number'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_range_count = {
		chart: {
			renderTo: 'container_range_count',
            text: 'range count'
        },
		title: {
            text: 'range count'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_leader_count = {
		chart: {
			renderTo: 'container_leader_count',
            text: 'leader count'
        },
		title: {
            text: 'leader count'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 数量'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Number'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};
var options_ttl = {
		chart: {
			renderTo: 'container_ttl',
            text: 'ttl'
        },
		title: {
            text: 'ttl'
        },
        legend: {
            layout: 'vertical',
            align: 'left',
            verticalAlign: 'top',
            x: 150,
            y: 100,
            floating: true,
            borderWidth: 1,
            backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
        },
        xAxis: {
        	type: 'datetime',
            dateTimeLabelFormats: {
                millisecond: '%Y-%m-%d, %H:%M:%S',
                day: '%Y-%m-%d'
            },
            plotBands: [{
                from: 4.5,
                to: 6.5,
                color: 'rgba(68, 170, 213, .2)'
            }]
        },
        yAxis: {
            title: {
                text: '空间 单位'
            }
        },
        tooltip: {
            shared: true,
            valueSuffix: ' Bytes'
        },
        credits: {
            enabled: false
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.5
            }
        },
    series: []       
};


//加载数据--按时间段查询
function queryFilter() {
	var type = "setOptions";
	load_monitor_data(type,options_cpu_percent,options_memory_percent,options_io_read_bytes,options_io_read_count,options_memory_swap,options_net_byte_recv,options_net_connection_count,options_net_drop_in,options_net_err_in,options_net_packet_recv,options_range_count,options_leader_count,options_ttl);
}
//加载数据
var type = "defaultOptions";
load_monitor_data(type,options_cpu_percent,options_memory_percent,options_io_read_bytes,options_io_read_count,options_memory_swap,options_net_byte_recv,options_net_connection_count,options_net_drop_in,options_net_err_in,options_net_packet_recv,options_range_count,options_leader_count,options_ttl);

function load_monitor_data(type,options_cpu_percent,options_memory_percent,options_io_read_bytes,options_io_read_count,options_memory_swap,options_net_byte_recv,options_net_connection_count,options_net_drop_in,options_net_err_in,options_net_packet_recv,options_range_count,options_leader_count,options_ttl) {
	Highcharts.setOptions({
		global: {
			useUTC: false
		}
	});
	//初始化
	var chart_cpu_percent = new Highcharts.Chart(options_cpu_percent);
	chart_cpu_percent.showLoading("Loading....");
	var chart_memory_percent = new Highcharts.Chart(options_memory_percent);
	chart_memory_percent.showLoading("Loading....");
	var chart_cpu_percent =  new Highcharts.Chart(options_cpu_percent);
	chart_cpu_percent.showLoading("Loading....");
	var chart_io_read_bytes =  new Highcharts.Chart(options_io_read_bytes);
	chart_io_read_bytes.showLoading("Loading....");
	var chart_io_read_count =  new Highcharts.Chart(options_io_read_count);
	chart_io_read_count.showLoading("Loading....");
	var chart_memory_percent = new Highcharts.Chart(options_memory_percent);
	chart_memory_percent.showLoading("Loading....");
	var chart_memory_swap =  new Highcharts.Chart(options_memory_swap);
	chart_memory_swap.showLoading("Loading....");
	var chart_net_byte_recv =  new Highcharts.Chart(options_net_byte_recv);
	chart_net_byte_recv.showLoading("Loading....");
	var chart_net_connection_count = new Highcharts.Chart(options_net_connection_count);
	chart_net_connection_count.showLoading("Loading....");
	var chart_net_drop_in =  new Highcharts.Chart(options_net_drop_in);
	chart_net_drop_in.showLoading("Loading....");
	var chart_net_err_in =   new Highcharts.Chart(options_net_err_in);
	chart_net_err_in.showLoading("Loading....");
	var chart_net_packet_recv = new Highcharts.Chart(options_net_packet_recv);
	chart_net_packet_recv.showLoading("Loading....");
	var chart_range_count =  new Highcharts.Chart(options_range_count);
	chart_range_count.showLoading("Loading....");
	var chart_leader_count = new Highcharts.Chart(options_leader_count);
	chart_leader_count.showLoading("Loading....");
	var chart_ttl =  new Highcharts.Chart(options_ttl);
	chart_ttl.showLoading("Loading....");
	
	//加载图
	var nodeId = $("#nodeId").val();
	var host = $("#host").val();
	var clusterId = $("#clusterId").val();
	var startTime = "";
	var endTime = "";
	if(type != "defaultOptions"){
		startTime = $("#start").val();
		endTime = $("#end").val();
	}
	$.ajax({
//        url:"/monitor/getNodeMonitorData",
		url:"/monitor/getNodeProcessMonitorData",
        type:"post",
        dataType:'json', 
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        data:{
        	"nodeId":nodeId,
        	"host":host,
        	"clusterId":clusterId,
        	"startTime":startTime,
        	"endTime":endTime
        },
        success: function(data){
        	if(data.code === 0){
        		chart_cpu_percent.addSeries(data.attach.cpu_percent);
        		chart_memory_percent.addSeries(data.attach.memory_percent);
        		
				chart_io_read_bytes.addSeries(data.attach.io_read_bytes);
				chart_io_read_bytes.addSeries(data.attach.io_write_bytes);
//				chart_io_write_bytes.addSeries(data.attach.io_write_bytes);
				
				chart_io_read_count.addSeries(data.attach.io_read_count);
				chart_io_read_count.addSeries(data.attach.io_write_count);
//				chart_io_write_count.addSeries(data.attach.io_write_count);
				chart_memory_swap.addSeries(data.attach.memory_swap);
				chart_memory_swap.addSeries(data.attach.memory_rss);
				chart_memory_swap.addSeries(data.attach.memory_vms);
//				chart_memory_rss.addSeries(data.attach.memory_rss);
//				chart_memory_vms.addSeries(data.attach.memory_vms);
				chart_net_byte_recv.addSeries(data.attach.net_byte_recv);
				chart_net_byte_recv.addSeries(data.attach.net_byte_sent);
//				chart_net_byte_sent.addSeries(data.attach.net_byte_sent);
				chart_net_connection_count.addSeries(data.attach.net_connection_count);
				
				chart_net_drop_in.addSeries(data.attach.net_drop_in);
				chart_net_drop_in.addSeries(data.attach.net_drop_out);
//				chart_net_drop_out.addSeries(data.attach.net_drop_out);
				chart_net_err_in.addSeries(data.attach.net_err_in);
				chart_net_err_in.addSeries(data.attach.net_err_out);
//				chart_net_err_out.addSeries(data.attach.net_err_out);
				chart_net_packet_recv.addSeries(data.attach.net_packet_recv);
				chart_net_packet_recv.addSeries(data.attach.net_packet_sent);
//				chart_net_packet_sent.addSeries(data.attach.net_packet_sent);
				chart_range_count.addSeries(data.attach.range_count);
				chart_leader_count.addSeries(data.attach.leader_count);
				chart_ttl.addSeries(data.attach.ttl);

//        		//系统负载
//	        	chart_load.addSeries(data.attach.ttl);
//	        	//出流量 、入流量
//	        	chart_outper_inper.addSeries(data.attach.bytes_out_per_sec);
//	        	chart_outper_inper.addSeries(data.attach.bytes_in_per_sec);
//	        	//cpu使用率
//	        	chart_cpu_rate.addSeries(data.attach.cpu_proc_rate);
//	        	//memory
//	        	chart_memory.addSeries(data.attach.total_memory);
//	        	chart_memory.addSeries(data.attach.used_memory);
//	        	//隐藏最大空间使用
//	        	var series_memory = chart_memory.series[1];
//	            if (series_memory.visible) {
//	                series_memory.hide();
//	            } else {
//	                series_memory.show();
//	            }
//	        	//disk
//	        	chart_disk.addSeries(data.attach.total_disk);
//	        	chart_disk.addSeries(data.attach.used_disk);
//	        	var series_disk = chart_disk.series[1];
//	            if (series_disk.visible) {
//	                series_disk.hide();
//	            } else {
//	                series_disk.show();
//	            }
//	        	//swap
//	        	chart_swap.addSeries(data.attach.total_swap_memory);
//	        	chart_swap.addSeries(data.attach.used_swap_memory);
//	        	//关闭loading
        		chart_cpu_percent.hideLoading();
        		chart_memory_percent.hideLoading();
				chart_io_read_bytes.hideLoading();
				chart_io_read_count.hideLoading();
				chart_memory_swap.hideLoading();
				chart_net_byte_recv.hideLoading(); 
				chart_net_connection_count.hideLoading();
				chart_net_drop_in.hideLoading();
				chart_net_err_in.hideLoading();
				chart_net_packet_recv.hideLoading();
				chart_range_count.hideLoading();
				chart_leader_count.hideLoading();
				chart_ttl.hideLoading();
//	        	chart_load.hideLoading();
//	        	chart_outper_inper.hideLoading();
//	        	chart_cpu_rate.hideLoading();
//	        	chart_memory.hideLoading(); 
//	        	chart_disk.hideLoading();
//	        	chart_swap.hideLoading();
        	}else{
        		chart_cpu_percent.showLoading("没有数据!"); 
        		chart_memory_percent.showLoading("没有数据!"); 
        		chart_io_read_bytes.showLoading("没有数据!"); 
        		chart_io_read_count.showLoading("没有数据!"); 
        		chart_memory_swap.showLoading("没有数据!"); 
        		chart_net_byte_recv.showLoading("没有数据!");  
        		chart_net_connection_count.showLoading("没有数据!"); 
        		chart_net_drop_in.showLoading("没有数据!"); 
        		chart_net_err_in.showLoading("没有数据!"); 
        		chart_net_packet_recv.showLoading("没有数据!"); 
        		chart_range_count.showLoading("没有数据!"); 
        		chart_leader_count.showLoading("没有数据!"); 
        		chart_ttl.showLoading("没有数据!"); 
//        		chart_load.showLoading("没有数据!");  
//        		chart_outper_inper.showLoading("没有数据!"); 
//        		chart_cpu_rate.showLoading("没有数据!"); 
//        		chart_memory.showLoading("没有数据!"); 
//        		chart_disk.showLoading("没有数据!"); 
//        		chart_swap.showLoading("没有数据!"); 
        	}
        },
        error: function(res){
        	chart_cpu_percent.showLoading("没有数据!"); 
        	chart_memory_percent.showLoading("没有数据!"); 
        	chart_io_read_bytes.showLoading("没有数据!"); 
        	chart_io_read_count.showLoading("没有数据!"); 
        	chart_memory_swap.showLoading("没有数据!"); 
        	chart_net_byte_recv.showLoading("没有数据!");  
        	chart_net_connection_count.showLoading("没有数据!"); 
        	chart_net_drop_in.showLoading("没有数据!"); 
        	chart_net_err_in.showLoading("没有数据!"); 
        	chart_net_packet_recv.showLoading("没有数据!"); 
        	chart_range_count.showLoading("没有数据!"); 
        	chart_leader_count.showLoading("没有数据!"); 
        	chart_ttl.showLoading("没有数据!"); 
//        	chart_load.showLoading("没有数据!");  
//    		chart_outper_inper.showLoading("没有数据!"); 
//    		chart_cpu_rate.showLoading("没有数据!"); 
//    		chart_memory.showLoading("没有数据!"); 
//    		chart_disk.showLoading("没有数据!"); 
//    		chart_swap.showLoading("没有数据!"); 
        }
    });
}