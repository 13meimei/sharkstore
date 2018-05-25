var index = 0;

function addPanel(keys, values) {
    $('#retTabs').tabs('add', {
        title: '结果集:' + index,
        content: '<table id="grid' + index + '"></table>',
        closable: true,
        border: false,
        width: 'auto',
        height: 'auto'
    });

    //escape html
    var columns = [];
    for (var i=0; i<keys.length; i++){
    	var column = [];
    	column.field = keys[i]
    	column.title = column.field;
    	column.width = 100;
    	column.formatter = html2Escape;
    	columns.push(column);
    }
    $('#grid'+index).datagrid({
    	fit:true,
    	data: values,
        method: 'post',
        columns: [columns],
        rownumbers: true,
        singleSelect:true,
        autoRowHeight:false,
        height:'auto'
//        onLoadSuccess: function(values){
//        }
    });

    index++;
}

function html2Escape(val,row,index) {
	if ((typeof(val)=='string' && val!='') && (val.indexOf('<')>-1 || val.indexOf('>')>-1)){
		return $('<div/>').text(val).html();
	}
	
	return val;
}

//获取textarea选中的值
$.fn.selection = function(){
	var textbox = this[0];
    if (document.selection){
        return document.selection.createRange().text;
     }else {
        return textbox.value.substring(textbox.selectionStart, textbox.selectionEnd);
     }
};

$.fn.setSelectRange = function( start, end ) {
	var textbox = this[0];
	if ( typeof textbox.createTextRange != 'undefined' ){
		var range = textbox.createTextRange();
		range.collapse( true); //移动插入光标到start处 
		range.moveStart( "character", start);
		range.moveEnd( "character", end-start);
		range.select();
	}else if ( typeof  textbox.setSelectionRange != 'undefined' ) { 
		textbox.setSelectionRange(start, end); 
	}
		textbox.focus(); 
	}

//datagrid(onHeaderContextMenu)
var HeaderContextMenu = function(e, field) {
    e.preventDefault();
    var headerContextMenu;
    if (!headerContextMenu) {
        var grid = $(this);
        var tmenu = $('<div style="width:100px;"></div>').appendTo('body');
        var fields = grid.datagrid('getColumnFields');
        for ( var i = 0; i < fields.length; i++) {
            var fildOption = grid.datagrid('getColumnOption', fields[i]);
            if (!fildOption.hidden) {
                $('<div iconCls="icon-ok" field="' + fields[i] + '"/>').html(fildOption.title).appendTo(tmenu);
            }else {
                $('<div iconCls="icon-empty" field="' + fields[i] + '"/>').html(fildOption.title).appendTo(tmenu);
            }
        }
        
        headerContextMenu = tmenu.menu({
            onClick : function(item) {
                var field = $(item.target).attr('field');
                if (item.iconCls == 'icon-ok') {
                    grid.datagrid('hideColumn', field);
                    $(this).menu('setIcon', {
                        target : item.target,
                        iconCls : 'icon-empty'
                    });
                } else {
                    grid.datagrid('showColumn', field);
                    $(this).menu('setIcon', {
                        target : item.target,
                        iconCls : 'icon-ok'
                    });
                }
            }
        });
    }
    
    headerContextMenu.menu('show', {
        left : e.pageX,
        top : e.pageY
    });
}

//datagrid(onRowContextMenu)
var RowContextMenu = function(e, rowIndex, rowData) {
    e.preventDefault();    
    var rowContextMenu;
    if (!rowContextMenu) {
        var grid = $(this);
        rowContextMenu = $('#clipMenu').menu({
            onClick : function(item) {
                var fields = grid.datagrid('getColumnFields');
                var text = '';
                for (var i=0; i<fields.length; i++){
                	if (i == (fields.length -1)){
                		text += fields[i] + "\r\n";
                	}else{
                		text += fields[i] + "\t";
                	}
                }
                
                if (item.iconCls == 'icon-copy') {
                	if (rowData){
                        for (var i=0; i<fields.length; i++){
                        	var key = fields[i];
                        	if (i == (fields.length -1)){
                        		text += rowData[key] + "\r\n";
                        	}else{
                        		text += rowData[key] + "\t";
                        	}
                        }
                	}
                }else if (item.iconCls == 'icon-copy2'){
                	var dataRows = grid.datagrid('getRows');
                	if (dataRows.length > 0){
                		for (var i=0; i<dataRows.length; i++){
                			var oneRow = dataRows[i];
                            for (var j=0; j<fields.length; j++){
                            	var key = fields[j];
                    	    	if (j == (fields.length -1)){
                    	    		text += oneRow[key] + "\r\n";
                    	    	}else{
                    	    		text += oneRow[key] + "\t";
                    	    	}
                			}
                		}
                	}
                }else{
                	alert('未知操作');
                	return false;
                }
                
                copyToClipboard(text);
            }
        });
    }
    
    rowContextMenu.menu('show', {
        left : e.pageX,
        top : e.pageY
    });
}

function copyToClipboard(txt) {
    if(window.clipboardData) {
            window.clipboardData.clearData();
            window.clipboardData.setData("Text", txt);
    } else if(navigator.userAgent.indexOf("Opera") != -1) {
         window.location = txt;
    } else if (window.netscape) {
         try {
              netscape.security.PrivilegeManager.enablePrivilege("UniversalXPConnect");
         } catch (e) {
              alert("被浏览器拒绝！\n请在浏览器地址栏输入'about:config'并回车\n然后将'signed.applets.codebase_principal_support'设置为'true'");
         }
         var clip = Components.classes['@mozilla.org/widget/clipboard;1'].createInstance(Components.interfaces.nsIClipboard);
         if (!clip)
              return;
         var trans = Components.classes['@mozilla.org/widget/transferable;1'].createInstance(Components.interfaces.nsITransferable); 
         if (!trans)
              return;   
         trans.addDataFlavor('text/unicode');
         var str = new Object();
         var len = new Object();
         var str = Components.classes["@mozilla.org/supports-string;1"].createInstance(Components.interfaces.nsISupportsString);
         var copytext = txt;
         str.data = copytext;
         trans.setTransferData("text/unicode",str,copytext.length*2);
         var clipid = Components.interfaces.nsIClipboard;   
         if (!clip)
              return false;
         clip.setData(trans,null,clipid.kGlobalClipboard);
         alert("复制成功！")
    }else{
    	alert('不支持的浏览器.');
    }
} 

$.fn.datagrid.defaults.onHeaderContextMenu = HeaderContextMenu;
$.fn.datagrid.defaults.onRowContextMenu = RowContextMenu;