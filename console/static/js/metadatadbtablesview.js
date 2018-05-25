/**
 * 数据库表列表展示
 */
var app = angular.module('myTablesInfo', []);
//集群监控数据获取
app.controller('tableInfo', function($rootScope, $scope, $http, $timeout, $compile) {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	//数据库id
	var dbId = $("#dbId").val();
	//数据库名字
	var dbName = $("#dbName").val();
	//集群id
	var clusterId = $('#clusterId').val();
	$("#tables_table_list").jqGrid({
		url:'/metadata/dbTablesDataGetByDbName?dbId='+dbId +'&dbName='+dbName +"&clusterId=" + clusterId,
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号','表名','创建时间','状态','自动扩容','自动failover','自动迁移','操作'],
		colModel : [ 
		    {name : 'id',index : 'id',align : "center",width : 5, sorttype : "int"}, 
			{name : 'name',index : 'name',width : 15,align : "center"},
			{name : 'create_time',index : 'create_time',width : 20,align : "center",
				formatter : function(value) {
					// 调用下面方法进行时间戳格式化
					return formatDate((new Date(value * 1000)),"yyyy-MM-dd hh:mm:ss");
				}
			},
			{name : 'state',index : 'state',width : 5,align : "center",
				formatter : function(value) {
					if(value == 0){
						return "不可用";
					}
					if(value == 1){
						return "初始化";
					}
					if(value == 2){
						return "正常";
					}
					if(value == 3){
						return "已删除";
					}
				}
			},
			{name : 'autoShardingUnable',index : 'autoShardingUnable',width : 9,align : "center",
	            edittype: 'checkbox',
	            formatter: "checkbox",
	            editoptions: { value:"True:False"},editable:true,formatoptions: {disabled : false}
	        },
			{name : 'autoTransferUnable',index : 'autoTransferUnable',width : 9,align : "center"},
			{name : 'autoFailoverUnable',index : 'autoFailoverUnable',width : 9,align : "center" },
			{name:'act',index:'act', width:30,sortable:false ,align : "center",title :""}
			],
		pager : "#tables_pager_list",
		viewrecords : true,
		caption : "数据库名:"+dbName,
		sortname : "id",
		sortorder: "desc",
		gridComplete: function(){
			var ids = jQuery("#tables_table_list").jqGrid('getDataIDs');
			for(var i=0;i < ids.length;i++){
				var rowId = ids[i];
				var rowData = jQuery("#tables_table_list").getRowData(rowId);
				var name = rowData.name;
				var id = rowData.id;
				viewRangeInfoTab = "<button class=\"btn btn-primary\" title=\"分片信息\" type=\"button\" value=\"分片信息\" onclick=\"viewRangePage('"+dbName+"','"+name+"');\">分片信息</button>&nbsp;&nbsp;&nbsp;&nbsp;"; 
				viewTableMonitor = "<button class=\"btn btn-primary\" title=\"表监控\" type=\"button\" value=\"表监控\" onclick=\"getTableMonitorInfo('"+dbName+"','"+id+"','"+name+"');\">表监控</button>&nbsp;&nbsp;&nbsp;&nbsp;"; 
				viewColumns = "<button class=\"btn btn-primary\" title=\"表结构\" type=\"button\" value=\"表结构\" onclick=\"getColumnsView('"+dbName+"','"+name+"');\">表结构</button>&nbsp;&nbsp;&nbsp;&nbsp;";
				viewTableEditColumns = "<button class=\"btn btn-primary\" title=\"修改\" type=\"button\" value=\"修改\" onclick=\"getTableEditView('"+dbName+"','"+name+"');\">修改</button>";
				var failover = "<input id=\"checkbox_id\" name=\"checkbox_id\" type=\"checkbox\" onclick=\"toggleAutoMigrate('"+rowId+"','"+dbName+"','"+name+"','autoFailoverUnable');\"/>";
				var transfer = "<input type=\"checkbox\" onclick=\"toggleAutoMigrate('"+rowId+"','"+dbName+"','"+name+"','autoTransferUnable');\"/>";
				//var sharding = "<input type=\"checkbox\" onclick=\"toggleAutoMigrate('"+rowId+"','"+dbName+"','"+name+"','autoShardingUnable');\"/>";
				jQuery("#tables_table_list").jqGrid('setRowData',ids[i],{autoFailoverUnable:failover});
				jQuery("#tables_table_list").jqGrid('setRowData',ids[i],{autoTransferUnable:transfer});
				//jQuery("#tables_table_list").jqGrid('setRowData',ids[i],{autoShardingUnable:sharding});
				jQuery("#tables_table_list").jqGrid('setRowData',ids[i],{act:viewRangeInfoTab+viewTableMonitor+viewColumns+viewTableEditColumns});
			}	
		},
        closeOnEscape:true,
		hidegrid : false
	});
	$("#tables_table_list").setSelection(4, true);
	$("#tables_table_list").jqGrid('navGrid', '#tables_pager_list', {
		edit : false,
		add : false,
		del : false,
		search : false,
		refresh: false
	}, {
		height : 200,
		reloadAfterSubmit : true,
		closeAfterAdd : true,
		closeAfterEdit : true
    });
	//删除按钮
	$("#tables_table_list").navButtonAdd('#tables_pager_list', {
        caption : "",
        buttonicon : "glyphicon glyphicon-trash",
        onClickButton : deleteRow,
        position : "last",
        title : "删除",
        cursor : "pointer"
    });
	// Add responsive to jqGrid
	$(window).bind('resize', function() {
		var width = $('.jqGrid_wrapper').width();
		$('#tables_table_list').setGridWidth(width);
	});
});
//$(document).ready(function() {
//	$.jgrid.defaults.styleUI = 'Bootstrap';
//	
//});
function toggleAutoMigrate1(evt) {
	alert(evt.target.type);
}
function toggleAutoMigrate(rowId,dbName,tableName, type){
	console.log(dbName);
	console.log(tableName);
	console.log(type);
	var rowData = jQuery("#tables_table_list").getRowData(rowId);
	console.log(rowData);
	var checkbox_id = jQuery("#checkbox_id").attr("checked"); 
	console.log(checkbox_id);
	//集群id
	var clusterId = $('#clusterId').val();
	//设置开关状态
	//上传数据
	data = {
		 autoShardingUnable:space['autoShardingUnable'],
		 autoTransferUnable:space['autoTransferUnable'],
		 autoFailoverUnable:space['autoFailoverUnable']
	}
	//提交开关设置
//	$http.post('/metadata/toggleAuto?clusterId=' + clusterId + '&dbName=' + dbName + '&tableName=' + tableName,data).success(function(data){
//      if(data.code != 200){
//        space[type] = !space[type];
//        alert("设置不成功，请稍后重试");
//      }
//    }).error(function(){
//      space[type] = !space[type];
//      alert("系统错误，请稍后重试");
//    });
 };
function deleteRow() {
	// 选中的行号
	var row = $("#tables_table_list").jqGrid('getGridParam', 'selrow');
	// 选中行数据
    var rowD = $("#tables_table_list").jqGrid("getRowData", row);
    //集群id
	var clusterId = $('#clusterId').val();
    var tableName = rowD.name;
    var dbName = rowD.db_name;
    if (row != null) {
    	jQuery("#tables_table_list").jqGrid('delGridRow', row, {
               	url : '/metadata/delTable?tableName='+tableName +'&dbName='+dbName +"&clusterId=" + clusterId,
                recreateForm : true,
                reloadAfterSubmit : true,
                closeAfterDelete : true
            });
    }else{
    	alert("请选择行!");
    }
}
//修改表结构
function getTableEditView(db_name,name) {
	//集群id
	var clusterId = $('#clusterId').val();
	window.location.href="/metadata/viewEditTable?dbName="+db_name+"&tableName="+name+"&clusterId=" + clusterId;
}
//时间格式化
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
//请求页面
function viewRangePage(db_name,name){
	//集群id
	var clusterId = $('#clusterId').val();
	window.location.href="/range/viewRangeInfo?db_name="+db_name+"&name="+name+"&clusterId=" + clusterId;
}
//获取table监控页面
function getTableMonitorInfo(dbName,tableId,tableName) {
	//集群id
	var clusterId = $('#clusterId').val();
	window.location.href="/monitor/viewTableMonitor?dbName="+dbName+"&tableName="+tableName+"&tableId="+tableId+"&clusterId=" + clusterId;
}
//表结构展示
function getColumnsView(db_name,name) {
	//集群id
	var clusterId = $('#clusterId').val();
	window.location.href="/page/metadata/viewTableColumns?db_name="+db_name+"&name="+name+"&clusterId=" + clusterId;
}