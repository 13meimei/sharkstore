/**
 * 数据库列表展示
 */
$(document).ready(function() {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	// Configuration for jqGrid Example 1
	var clusterId = $('#clusterId').val();
	$("#db_table_list").jqGrid({
		url:'/metadata/dbDataGetAllViewPage?clusterId=' + clusterId,
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号', '数据库名','操作'],
		colModel : [ 
		    {name : 'id',index:'id',align : "center",width : 20, sorttype : "int"}, 
			{name : 'name',index:'name',width : 90,align : "center"}, 
			{name:'act',index:'act', width:35,sortable:false ,align : "center"}
			],
		pager : "#db_pager_list",
		viewrecords : true,
		caption : "数据库列表",
		sortname : "id",
		sortorder: "desc",
		gridComplete: function(){
			var ids = jQuery("#db_table_list").jqGrid('getDataIDs');
			for(var i=0;i < ids.length;i++){
				var rowId = ids[i];
				var rowData = jQuery("#db_table_list").getRowData(rowId);
				var dbId = rowData.id;
				var dbName = rowData.name;
				dbConsole = "<button class=\"btn btn-primary\" type=\"button\" title=\"控制台\" value==\"控制台\" onclick=\"viewConsole("+dbId+",'"+dbName+"');\">控制台</button>&nbsp;&nbsp;&nbsp;&nbsp;";
				createTable = "<button class=\"btn btn-primary\" type=\"button\" title=\"创建表\" value=\"创建表\" onclick=\"viewcreateTable("+dbId+",'"+dbName+"');\">创建表</button> &nbsp;&nbsp;&nbsp;&nbsp;";
				viewTable = "<button class=\"btn btn-primary\" type=\"button\" title=\"查看表\" value=\"查看表\" onclick=\"getTableInfo("+dbId+",'"+dbName+"');\">查看表</button>"; 
				jQuery("#db_table_list").jqGrid('setRowData',ids[i],{act:dbConsole+createTable+viewTable});
			}	
		},
		hidegrid : false
	});

	// Add responsive to jqGrid
	$(window).bind('resize', function() {
		var width = $('.jqGrid_wrapper').width();
		$('#db_table_list').setGridWidth(width);
	});
});

/**
 * 展示console页面
 * 执行数据库查询
 */
function viewConsole(dbId,dbName) {
	window.location.href="/console/viewConsole?dbName="+dbName+"&dbId="+dbId;
}
/**
 * 根据数据库dbName 、 dbid 创建tables
 * 跳转到tables页面
 * @param id
 */
function viewcreateTable(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/metadata/viewCreateTable?dbName="+dbName+"&dbId="+dbId+"&clusterId="+clusterId;
}

function getTableInfo(dbId,dbName) {
	var clusterId = $('#clusterId').val();
	window.location.href="/page/tables/tablelist?dbName="+dbName+"&dbId="+dbId+"&clusterId="+clusterId;
}