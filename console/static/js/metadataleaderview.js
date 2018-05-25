/**
 * leader 信息列表展示
 */
$(document).ready(function() {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	// Configuration for jqGrid Example 1
	$("#leader_table_list").jqGrid({
		url:'/metadata/getLeaderData',
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号', '数据库名', 'Properties','操作'],
		colModel : [ 
		    {name : 'Id',index : 'Id',align : "center",width : 20, sorttype : "int"}, 
			{name : 'Name',index : 'Name',width : 90,align : "center"}, 
			{name : 'Properties',index : 'Properties',align : "center",width : 100}, 
			{name:'act',index:'act', width:35,sortable:false ,align : "center"},
			],
		pager : "#leader_pager_list",
		viewrecords : true,
		caption : "leader列表",
		sortname : "id",
		sortorder: "desc",
		hidegrid : false
	});

	// Add responsive to jqGrid
	$(window).bind('resize', function() {
		var width = $('.jqGrid_wrapper').width();
		$('#leader_table_list').setGridWidth(width);
	});
});