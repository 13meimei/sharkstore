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

var app = angular.module('myClusterTask', []);

//历史任务列表操作
app.controller('historyClusterTask', function($rootScope, $scope, $http, $timeout) {
	var clusterId = $('#clusterId').val();
	var startTime = $('#start').val();
	var endTime = $('#end').val();
	var bt_data;
	$.ajax({
		url:"/task/getHistoryTaskById",
		type:"post",
		async: false,
        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
        dataType:"json",
        data:{
        	"clusterId":clusterId,
        	"startTime":startTime,
        	"endTime":endTime
        	
        },
		success: function(data){
			if(data.code === 200){
				bt_data = data.attach;
			}
        }
	});
	var options = {
	    	data: bt_data,
	    	columns: [
	                  {
	                	  title: '任务描述',
	                      field: 'detail',
	                      align: 'left',
	                      valign: 'left',
	                  }, {
	                      title: '创建时间',
	                      field: 'createTime',
	                      align: 'center',
	                      valign: 'middle'
	                  }, {
	                      title: '耗时(秒)',
	                      field: 'usedTime',
	                      align: 'center',
	                      valign: 'middle'
	                  }, {
	                      title: '完成时间',
	                      field: 'finishTime',
	                      align: 'center',
	                      valign: 'middle'
	                  }, {
	                      title: '状态',
	                      field: 'state',
	                      align: 'center',
	                      valign: 'middle'
	                  }
	              ],
	      pagination: true,
	      iconSize: 'outline'
	    };
    $('#viewhistorytasklist').bootstrapTable(options);
    //根据时间段查询任务列表
    $scope.queryFilter = function() {
    	var clusterId = $('#clusterId').val();
    	var startTime = $('#start').val();
    	var endTime = $('#end').val();
    	$.ajax({
    		url:"/task/getHistoryTaskById",
    		type:"post",
    		async: false,
            contentType:"application/x-www-form-urlencoded; charset=UTF-8",
            dataType:"json",
            data:{
            	"clusterId":clusterId,
            	"startTime":startTime,
            	"endTime":endTime
            },
    		success: function(data){
    			if(data.code === 0){
    				options.data = data.data;
    				//重新加载数据
    				$('#viewhistorytasklist').bootstrapTable('load',options);
    			}
            }
    	});
	};
});
//集群当前任务列表
app.controller('presentClusterTask', function($rootScope, $scope, $http, $timeout) {
	var clusterId = $('#clusterId').val();
	var options = {
			    	data: [],
			    	columns: [
			    	  {
	                	  title: '任务描述',
	                      field: 'describe',
	                      align: 'left',
	                      valign: 'left',
	                  // }, {
	                  //     title: '创建时间',
	                  //     field: 'createtime',
	                  //     align: 'center',
	                  //     valign: 'middle',
	                  //     formatter : function(cellValue) {
						// 		// 调用下面方法进行时间戳格式化
						// 		return formatDate((new Date(cellValue * 1000)),"yyyy-MM-dd hh:mm:ss");
						// 	}
	                  },{
	                      title: '状态',
	                      field: 'state',
	                      align: 'center',
	                      valign: 'middle'
	                  }
	              ],
	      pagination: true,
	      iconSize: 'outline'
	    };
		$.ajax({
			url:"/task/getPresentTaskById",
			type:"post",
			async: false,
	        contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	        	"type":"allTask",
	        	"clusterId":clusterId
	        },
			success: function(data){
				if(data.code === 0){
					options.data = data.data;
				}
	        }
		});
		$('#viewpresentlist').bootstrapTable(options);
});