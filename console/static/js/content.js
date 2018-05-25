//判断当前页面是否在iframe中
if (top == this) {
	var gohome = '<div class="gohome"><a class="animated bounceInUp" href="index.html?v=4.0" title="返回首页"><i class="fa fa-home"></i></a></div>';
	$('body').append(gohome);
}

//animation.css
function animationHover(element, animation) {
	element = $(element);
	element.hover(function() {
		element.addClass('animated ' + animation);
	}, function() {
		//动画完成之前移除class
		window.setTimeout(function() {
			element.removeClass('animated ' + animation);
		}, 2000);
	});
}

//拖动面板
function WinMove() {
	var element = "[class*=col]";
	var handle = ".ibox-title";
	var connect = "[class*=col]";
	$(element).sortable({
		handle : handle,
		connectWith : connect,
		tolerance : 'pointer',
		forcePlaceholderSize : true,
		opacity : 0.8,
	}).disableSelection();
};
