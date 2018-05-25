$(document).ready(function() {
	$.jgrid.defaults.styleUI = 'Bootstrap';
	var dbName = $('#dbName').val();
	var name = $('#name').val();
	$("#range_table_list").jqGrid({
		url:'/range/getRangeByBbTable?db_name='+dbName +'&name='+name,
		datatype: "json",
		height : "50%",
		autowidth : true,
		shrinkToFit : true,
		rowNum : 10,
		rowList : [ 10, 20, 30 ],
		colNames : [ '序号', 'start_key', 'end_key', 'range_id','node_id','address','心跳地址','复制地址'],
		colModel : [ 
		    {name : 'id',index : 'id',align : "center",width : 20}, 
		    {name : 'start_key',index : 'start_key',width : 90,align : "center"}, 
		    {name : 'end_key',index : 'end_key',width : 90,align : "center"}, 
		    {name : 'range_id',index : 'node_id',width : 90,align : "center"}, 
		    {name : 'node_id',index : 'id',width : 90,align : "center"}, 
			{name : 'address',index : 'address',width : 90,align : "center"}, 
			{name : 'heartbeat_addr',index : 'heartbeat_addr',align : "center",width : 100}, 
			{name : 'replicate_addr',index : 'replicate_addr',align : "center",width : 100}
			],
		pager : "#range_pager_list",
		viewrecords : true,
		caption : "range列表",
		sortname : "start_key",
		sortorder: "desc",
		hidegrid : false
	});

	// Add responsive to jqGrid
	$(window).bind('resize', function() {
		var width = $('.jqGrid_wrapper').width();
		$('#range_table_list').setGridWidth(width);
	});
});