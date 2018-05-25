app = angular.module('clusterDeployPolicy', []);
app.controller('clusterDeployPolicyCtl', ['$rootScope', '$scope', '$http', '$timeout', '$compile',function($rootScope, $scope, $http, $timeout, $compile){
	$scope.clusterDeployPolicy = function() {
		//获取当前设置
		var clusterId = $('#clusterId').val();
		$.ajax({
			url:"/cluster/clusterDeployPolicy",
			type:"post",
			async: false,
			contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	        	"clusterId":clusterId,
	        	"type":"get"
	        },
			success: function(data){
				$scope.clusterdps = data.attach;
	        },
	        error: function(res){
	            alert(res.message);
	        }
		});
		$('#subDetail').modal('show', {backdrop: 'static'});
	};
	$scope.addSource = function (type) {
		var clusterId = $('#clusterId').val();
		if(type== 'addsource'){
			window.location.href = "/metadata/resources?clusterId="+ clusterId;
		}
		if(type== 'createdb'){
			window.location.href = "/page/metadata/metadatacreatedb?clusterId="+ clusterId;
		}
		if(type== 'createtable'){
			window.location.href = "/page/metadata/metadatacreatetable?clusterId="+ clusterId;
		}
	};
	//添加行
	$scope.slaveAdd = function() {
		var _len = $("#one tr").length;
		var htmltext = "<tr id="+_len+">" +
						'<td style="height: 32px;">' +
							"<select class=\"form-control\" style=\"height:35px;\" name=\"slave"+_len+"\" id=\"slave"+_len+"\">" +
								'<option value="廊坊">廊坊</option>' +
								'<option value="马驹桥">马驹桥</option>' +
								'<option value="廊坊润泽">廊坊润泽</option>' +
			                	'<option value="廊坊联通磐石">廊坊联通磐石</option>' +
							'</select>' +
						 '</td>' +
						 '<td style="width:95%">' +
						 	"<input class=\"form-control\" style=\"height:35px;\" type=\"number\" min=\"1\" value=\"\" id=\"slave_ds_num"+_len+"\" name=\"slave_ds_num"+_len+"\" placeholder=\"副本数量\"/>" +
						 '</td>' +
						 '<td style="width:10px;border: 0px solid #804040;">' +
						 	"<i class=\"fa fa-minus ml50\" ng-click=\"slaveDel("+_len+")\"></i>" +
						 '</td>' +
						'</tr>';
		var tpl = $compile(htmltext);
	    var e = tpl($scope);
	    $('#one').append(e);
	};
	//删除当前行
	$scope.slaveDel = function(index) {
		$scope.clusterdps.master.slaves.splice(index -2,1);
		$("tr[id='"+index+"']").remove();
	};
	//保存数据
	$scope.saveClusterDeployPolicy = function() {
		var clusterId = $('#clusterId').val();
		var zone_name = $('#zone_name').val();
		var _len = $("#one tr").length;
		var master = $('#master').val();
		var ds_num = $('#ds_num').val();
		if(_len > 2){
			var array = new Array();
			for(var i = 2; i < _len; i++){
				var slavelist = {};
				var slave = $('#slave'+ i ).val();
				var slave_ds_num = $('#slave_ds_num'+ i).val();
				slavelist.master = slave;
				slavelist.ds_num = parseInt(slave_ds_num);
				slavelist.sync = 1;
				array.push(slavelist);
			}
		}
		var masterJson = {};
		masterJson.room_name = master;
		masterJson.ds_num = parseInt(ds_num);
		masterJson.sync = 1;
		//组装数据
		var allJson = {};
		allJson.zone_name = zone_name;
		allJson.master = masterJson;
		allJson.slaves = array;
		var masterJson = {};
		masterJson.master = allJson;
		$.ajax({
			url:"/cluster/clusterDeployPolicy",
			type:"post",
			contentType:"application/x-www-form-urlencoded; charset=UTF-8",
	        dataType:"json",
	        data:{
	        	"clusterId":clusterId,
	        	"type":"set",
	        	"deployPolicy":JSON.stringify(masterJson)
	        },
			success: function(data){
	            alert(data.attach.message);
	        },
	        error: function(res){
	            alert(res.message);
	        }
		});
	};
}]);
