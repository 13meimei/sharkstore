/**
 * 1、执行数据库查询
 * 2、sql 和 输入效验
 */
function btnQuery() {
	 if ($('#qrySql').val() == '') {
         $.messager.alert('Warning', '请填写正确的SQL语句');
         return false;
     }
	 if ($('#dbUserName').val() == '') {
         $.messager.alert('Warning', '请填写用户名');
         return false;
     } 
	 if ($('#dbPassWord').val() == '') {
         $.messager.alert('Warning', '请填写密码');
         return false;
     } 
     //选中文本,并设置选中文字
     var selectTxt = $('#qrySql').selection();
     if (selectTxt == '') {
         selectTxt = $('#qrySql').val();
     } else {
         var start = $('#qrySql').val().indexOf(selectTxt);
         var end = start + selectTxt.length;
         $('#qrySql').setSelectRange(start, end);
     }
     if('' == selectTxt){
    	 $.messager.alert('Warning', '请输入sql');
         return false;
     }
      var params = {};
      params.sql = selectTxt;
      params.dbName = $('#dbName').val();
      params.dbUserName = $('#dbUserName').val();
      params.dbPassWord = $('#dbPassWord').val();
      params.clusterId = $('#clusterId').val();   
      //通过ajax进行数据查询
      $.ajax({
    	  url: "/db/console/query",
          type: "post",
          contentType:"application/x-www-form-urlencoded",
          data: params,
          dataType: "json",
          cache: false,
          beforeSend: function (XMLHttpRequest) {
              $.messager.progress();
          },
          success: function (data, textStatus) {
              if (data.code == 0) {
                  addPanel(data.data.keys, data.data.values);
              } else {
                  $.messager.alert('Warning', data.msg);
              }
          },
          error: function (XMLHttpRequest, textStatus, errorThrown) {
              if (XMLHttpRequest.status >= 500) {
                  textStatus = '查询超时，请重试，或联系此数据库DBA人员.';
              }

              $.messager.alert('Error', XMLHttpRequest.status + ':' + textStatus);
          },
          complete: function (XMLHttpRequest, textStatus) {
              $.messager.progress('close');
          }
      });
}
