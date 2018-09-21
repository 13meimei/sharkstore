/*!
 * 集群展示页面操作
 */

var app = angular.module('sysCluster', []);
//集群监控数据获取
app.controller('myFbaseClusterInfo', function($rootScope, $scope, $http, $timeout) {
	 $http.get('/cluster/queryClusters').success(function(data){
		 if(data.code === 0){
			 $scope.clusterList = data.data;
		 }else {
			swal("获取失败", data.msg, "error");
		 }
	 });
	 //维护集群基础信息
     $scope.editClusterInfo = function(space){
         $("#clusterId").val(space.id);
         $("#clusterName").val(space.name);
         $("#masterUrl").val(space.master_url);
         $("#gatewayHttpUrl").val(space.gateway_http);
         $("#gatewaySqlUrl").val(space.gateway_sql);
         $("#token").val(space.cluster_sign);
         $('#clusterDetailModal').modal('show');
     };
    //集群调度
	 $scope.scheduleAdjust = function(space){
        window.location.href="/page/system/scheduleAdjust?clusterId=" + space.id;
	 };
    //集群拓扑
    $scope.topologyView = function(space){
        window.location.href="/page/cluster/topology?clusterId=" + space.id;
    };

    $scope.viewRangeTopoByRngId = function (space) {
        swal({
                title: "查看range拓扑",
                text: "请输入一个rangeId来查看",
                type: "input",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "查看",
                closeOnConfirm: false
            },
            function (inputValue) {
                if (inputValue === false) return;
                if (inputValue === "") {
                    swal.showInputError("你需要输入rangeId");
                    return
                }

                var rangeId = inputValue;
                if (rangeId == "" || rangeId == null) {
                    swal("请输入rangeId");
                    return
                }
                window.location.href = window.location.href = "/range/getRangeTopo?clusterId=" + space.id + "&rangeId=" + rangeId;
            }
        );
    };

    $scope.viewRangeOpsTopN = function (space) {
        swal({
                title: "输入查看的条数，不填默认top10",
                text: "请输入查看的条数",
                type: "input",
                showCancelButton: true,
                confirmButtonColor: "#DD6B55",
                confirmButtonText: "查看",
                closeOnConfirm: false
            },
            function (inputValue) {
                if (inputValue === false) return;
                if (inputValue === "") {
                    inputValue = 10;
                }
                var topN = inputValue;
                window.location.href = "/page/cluster/viewRangeOpsTopN?clusterId=" + space.id + "&topN=" + topN;
            }
        );
    };
});

function saveInfo() {
    var clusterId = $("#clusterId").val();
    if (!hasText(clusterId)) {
        swal("没有获取到集群信息，请联系开发人员")
        return
    }
    var clusterName = $("#clusterName").val();
    var masterUrl = $("#masterUrl").val();
    var gatewayHttpUrl = $("#gatewayHttpUrl").val();
    var gatewaySqlUrl = $("#gatewaySqlUrl").val();
    if (!hasText(clusterName) || !hasText(masterUrl) || !hasText(gatewayHttpUrl) || !hasText(gatewaySqlUrl)) {
            swal("修改", "集群名、masterUrl、网关http、网关sql是必填项", "error");
            return
    }
    var token = $("#token").val();
    swal({
          title: "修改集群?",
          type: "warning",
          showCancelButton: true,
          confirmButtonColor: "#DD6B55",
          confirmButtonText: "更新",
          closeOnConfirm: false
        },function(){
            //执行ajax提交
            $.ajax({
                url:"/cluster/createCluster",
                type:"post",
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                dataType:"json",
                data:{
                    clusterId:clusterId,
                    clusterName:clusterName,
                    masterUrl:masterUrl,
                    gatewayHttpUrl:gatewayHttpUrl,
                    gatewaySqlUrl:gatewaySqlUrl,
                    token:token
                },
                success: function(data){
                    if(data.code === 0){
                        swal("修改成功!", data.msg, "success");
                        $('#clusterDetailModal').modal('hide');
                          window.location.href="/page/system/scheduleManage";
                    }else {
                        swal("修改集群失败", data.msg, "error");
                    }
                },
                error: function(res){
                    swal("修改集群失败", res, "error");
                }
            });
    });
}

function delInfo() {
    var clusterId = $("#clusterId").val();
    if (!hasText(clusterId)) {
        swal("没有获取到集群信息，请联系开发人员");
        return
    }
    if(clusterId == 1){
        swal("基础集群只能修改，不能删除");
        return
    }
    swal({
          title: "删除集群?",
          type: "warning",
          showCancelButton: true,
          confirmButtonColor: "#DD6B55",
          confirmButtonText: "删除",
          closeOnConfirm: false
        },function(){
            //执行ajax提交
            $.ajax({
                url:"/cluster/deleteCluster?clusterId=" + clusterId,
                type:"get",
                contentType:"application/x-www-form-urlencoded; charset=UTF-8",
                success: function(data){
                    if(data.code === 0){
                        swal("删除成功!", data.msg, "success");
                        $('#clusterDetailModal').modal('hide');
                        window.location.href="/page/system/scheduleManage";
                    }else {
                        swal("删除集群失败", data.msg, "error");
                    }
                },
                error: function(res){
                    swal("删除集群失败", res, "error");
                }
            });
    });
}