var app = angular.module('unhealthyRanges', []);
//集群监控数据获取
app.controller('myranges', function($rootScope, $scope, $http, $timeout) {
     var clusterId = $('#clusterId').val();
	 var dbName = $("#dbName").val();
     var tableName = $("#tableName").val();
    var rangeId = $("#rangeId").val();
	 $http.get('/range/getUnhealthyRanges?tableName='+tableName +'&dbName='+dbName +'&clusterId=' + clusterId+'&rangeId=' + rangeId).success(function(data){
	 	if(data.code === 0){
			$scope.unhealthyRanges = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });

	  $scope.viewPeerInfo = function(rangeId) {
	        window.location.href="/page/range/peerinfo?rangeId="+ rangeId +"&tableName="+tableName+"&dbName="+dbName+"&clusterId="+clusterId;
       };
	  $scope.rebuildRange = function(rangeId) {
          $.ajax({
              url:"/range/rebuildRange",
              type:"post",
              async: false,
              contentType:"application/x-www-form-urlencoded; charset=UTF-8",
              dataType:"json",
              data:{
                  "dbName":dbName,
                  "tableName":tableName,
                  "rangeId":rangeId,
                  "clusterId":clusterId
              },
              success: function(data){
                  if(data.code === 0){
                      swal("range重建成功！", "range重建成功!", "success");
                      window.location.reload();
                  }else {
                      swal("range重建失败", data.msg, "error");
                  }
              },
              error: function(res){
                  swal("range重建失败", res, "error");
              }
          });
	  };

 });
