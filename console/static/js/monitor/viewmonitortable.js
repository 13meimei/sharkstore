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
var options_ops = {
        chart: {
        	renderTo: 'container_ops',
            type: 'spline'            
        },
        title: {
            text: '表 OPS'
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
                text: '表 OPS'
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
            pointFormat: '{point.x:%e. %b}: {point.y:.2f} ops'
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
        	renderTo: 'container_outper_inper',
            type: 'spline'            
        },
		title: {
            text: '表出入流量监控'
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
                text: '出入流量 '
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
var options_size = {
		chart: {
			renderTo: 'container_size',
            text: '表占用空间'
        },
		title: {
            text: '表占用空间'
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
                text: '表占用空间 '
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e. %b}: {point.y:.2f} 表占用空间'
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
var options_range_count = {
		chart: {
			renderTo: 'container_range_count',
            text: '表分片数量'
        },
		title: {
            text: '表分片数量'
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
                text: '表分片数量 '
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e. %b}: {point.y:.2f} 分片数量'
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

//加载数据--按时间段查询
function queryFilter() {
	var type = "setOptions";
	load_monitor_data(type,options_ops,options_outper_inper,options_size,options_range_count);
}
//加载数据
var type = "defaultOptions";
load_monitor_data(type,options_ops,options_outper_inper,options_size,options_range_count);

function load_monitor_data(type,options_ops,options_outper_inper,options_size,options_range_count) {
	Highcharts.setOptions({
		global: {
			useUTC: false
		}
	});
	//初始化
	var chart_ops = new Highcharts.Chart(options_ops);
	var chart_outper_inper = new Highcharts.Chart(options_outper_inper);
	var chart_size = new Highcharts.Chart(options_size);
	var chart_range_count = new Highcharts.Chart(options_range_count);
	//加载动画
	chart_ops.showLoading("Loading....");  
	chart_outper_inper.showLoading("Loading...."); 
	chart_size.showLoading("Loading...."); 
	chart_range_count.showLoading("Loading...."); 
	var tableId = $("#tableId").val();
	var tableName = $("#tableName").val();
	var dbName = $("#dbName").val();
	var clusterId = $('#clusterId').val();
	var startTime = "";
	var endTime = "";
	if(type != "defaultOptions"){
		startTime = $("#start").val();
		endTime = $("#end").val();
	}
	$.ajax({
        url:"/monitor/getTableMonitorData",
        type:"post",
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
        	"dbName":dbName,
        	"tableName":tableName,
        	"tableId":tableId,
        	"clusterId":clusterId,
        	"startTime":startTime,
        	"endTime":endTime
        },
        success: function(data){
        	if(data.code === 0){
        		//db ops
	        	data.attach.ops.name = data.attach.ops.name;
	        	chart_ops.addSeries(data.attach.ops);
	        	//出流量
	        	data.attach.bytes_out_per_sec.name = data.attach.bytes_out_per_sec.name+" 出流量";
	        	//入流量
	        	data.attach.bytes_in_per_sec.name = data.attach.bytes_in_per_sec.name+" 入流量";
	        	chart_outper_inper.addSeries(data.attach.bytes_out_per_sec);
	        	chart_outper_inper.addSeries(data.attach.bytes_in_per_sec);
	        	//db 占用磁盘大小
	        	data.attach.size.name = data.attach.size.name;
	        	chart_size.addSeries(data.attach.size);
	        	//range_count
	        	data.attach.range_count.name = data.attach.range_count.name;
	        	chart_range_count.addSeries(data.attach.range_count);
	        	//关闭loading
	        	chart_ops.hideLoading();
	        	chart_outper_inper.hideLoading();
	        	chart_size.hideLoading();
	        	chart_range_count.hideLoading();
        	}else{
        		chart_ops.showLoading("没有数据!"); 
        		chart_outper_inper.showLoading("没有数据!");
        		chart_size.showLoading("没有数据!");
        		chart_range_count.showLoading("没有数据!");
        	}
        },
        error: function(res){
        	chart_ops.showLoading("监控异常!");  
    		chart_outper_inper.showLoading("监控异常!"); 
    		chart_size.showLoading("监控异常!"); 
    		chart_range_count.showLoading("监控异常!"); 
        }
    });
}