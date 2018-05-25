/**
 * 数据库列表展示
 */
$(document).ready(function() {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	// Configuration for jqGrid Example 1
	$("#db_table_list").jqGrid({
		url:'/metadata/dbDataGetAll',
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号', '数据库名','操作'],
		colModel : [ 
		    {name : 'id',index:'id',align : "center",width : 20, sorttype : "int"}, 
			{name : 'name',index:'name',width : 20,align : "center"}, 
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
				viewTable = "<button class=\"btn btn-primary\" type=\"button\" title=\"查看表\" value==\"查看表\" onclick=\"getTableInfo("+dbId+",'"+dbName+"');\">查看表</button>&nbsp;&nbsp;&nbsp;&nbsp;";
				jQuery("#db_table_list").jqGrid('setRowData',ids[i],{act:dbConsole+viewTable});
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
 * 根据数据库dbName 、 dbid 查询tables
 * 跳转到tables页面
 * @param id
 */
function getTableInfo(dbId,dbName) {
	window.location.href="/metadata/viewDataBaseTablesInfo?dbName="+dbName+"&dbId="+dbId;
}
/**
 * 展示console页面
 * 执行数据库查询
 */
function viewConsole(dbId,dbName) {
	window.location.href="/console/viewConsole?dbName="+dbName+"&dbId="+dbId;
}















