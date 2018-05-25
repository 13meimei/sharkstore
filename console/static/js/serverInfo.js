/**
 * Created by liushenghui on 2017/4/5.
 */
$(document).ready(function() {
    $.jgrid.defaults.styleUI = 'Bootstrap';
    // Configuration for jqGrid Example 1
    var re =  /^([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])$/;
    function mycheck(value) {
        if(isNaN(parseFloat(value)) ||!re.test(value)) {
            // alert(parseFloat(value))
            return [false,"请输入正确的ip",""];
        } else {
            return [true,"",""];
        }
    }
    $("#server_table_list").jqGrid({
        url: '/serverInfo/getServerList',
        datatype: "json",
        height: "50%",
        autowidth: true,
        shrinkToFit: true,
        rowNum: 10,
        rowList: [10, 20, 30],
        colNames: [ 'ID', 'IP','创建时间','状态'],
        colModel: [
            {
                name: 'id',
                index: 'id',
                editable: false,
                    align: "center",
                width: 25,
                search:false,
                hidden:true
            },
            {

                name: 'ip',
                index: 'ip',
                editable: true,
                align: "center",
                width: 25,
                editrules:{custom:true,custom_func:mycheck}
            },
            {
                name: 'createdTime',
                index: 'createdTime',
                editable: false,
                width: 25,
                align: "center",
                search:false,
                formatter: function (cellValue) {
                    //调用下面方法进行时间戳格式化
                    return formatDate((new Date(cellValue)), "yyyy-MM-dd hh:mm:ss");
                }
            },
            {
                name: 'status',
                index: 'status',
                editable: true,
                width: 10,
                align: "center",
                formatter: "select",
                edittype : "select",
                editoptions: {value: "0:正常;1:报修中"}
            }
        ],
        pager: "#server_pager_list",
        viewrecords: true,
        caption: "服务器列表",
        add: true,
        edit: true,
        addtext: 'Add',
        edittext: 'Edit',
        editurl: '/serverInfo/editServerInfo',
        hidegrid: false,
        closeOnEscape:true,
        rownumbers:true
    })
    // Add selection
    $("#server_table_list").setSelection(4, true);
    // Setup buttons
    $("#server_table_list").jqGrid('navGrid', '#server_pager_list', {
        edit: true,
        add: true,
        del: true,
        search: true
    }, {
        height: 200,
        reloadAfterSubmit : true,
		closeAfterAdd : true,
		closeAfterEdit : true
    }).navSeparatorAdd("#server_pager_list").jqGrid('navButtonAdd',"#server_pager_list",{
        caption:"批量添加",
        buttonicon:"ui-icon-newwin",
        onClickButton:firm,
        position: "last",
        title:"请上传excel文件",
        modal:true,
        cursor: "pointer"
    });
    // Add responsive to jqGrid
    $(window).bind('resize', function () {
        var width = $('.jqGrid_wrapper').width();
        $('#server_table_list').setGridWidth(width);
    });
});


//格式化日期,
function formatDate(date,format) {
    var paddNum = function (num) {
        num += "";
        return num.replace(/^(\d)$/, "0$1");
    }
    //指定格式字符
    var cfg = {
        yyyy: date.getFullYear() //年 : 4位
        , yy: date.getFullYear().toString().substring(2)//年 : 2位
        , M: date.getMonth() + 1  //月 : 如果1位的时候不补0
        , MM: paddNum(date.getMonth() + 1) //月 : 如果1位的时候补0
        , d: date.getDate()   //日 : 如果1位的时候不补0
        , dd: paddNum(date.getDate())//日 : 如果1位的时候补0
        , hh: date.getHours()  //时
        , mm: date.getMinutes() //分
        , ss: date.getSeconds() //秒
    }
    format || (format = "yyyy-MM-dd hh:mm:ss");
    return format.replace(/([a-z])(\1)*/ig, function (m) {
        return cfg[m];
    });
}
//ip添加

function ipadd() {
    //展示弹出框
    $('#ipModal').modal('show');
}

//批量上传操作js
function  firm()
{
	//展示弹出框
	$('#identifier').modal('show');
}
function createip() {
	//获取页面所有ip
    var iplist = $("#iplist").val();
    iplist = $.trim(iplist);
    var iplistTemp = iplist.split("\n");
    var ip = iplistTemp.length;
    //IP正则
    var re =  /^([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.([0-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])$/;
    while (ip--) {
        console.log(iplistTemp[ip]);
        if(!re.test(iplistTemp[ip])){
            alert("请输入正确的IP");
            return;
    }
        }
    $.ajax({
        url:"/serverInfo/createIp",
        type:"post",
        contentType:"application/json;charset=utf-8",
        dataType:"json",
        data:JSON.stringify(iplistTemp),
        success: function(data){
            if(data.code === 0){
                alert("插入成功")
            }
        }
    });
}