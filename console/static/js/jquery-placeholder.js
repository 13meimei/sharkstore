$.fn.placeholder = function(){
	return this.each(function () {
		if(Modernizr.input["placeholder"] == false){
			var _self = $(this);
			var phv = _self.attr('placeholder');
			var color = '#777777';
			//div setup
			var placeholder = $('<div>'+phv+'</div>');
			$('body').append(placeholder);
			var top = _self.offset().top + ((_self.outerHeight() - _self.height()) / 2);
			var left = _self.offset().left + ((_self.outerWidth() - _self.width()) / 2);
			var width = _self.outerWidth();
			var height =  _self.outerHeight();
			var zIndex = _self.css('zIndex');
			if(zIndex != 'auto'){
				zIndex = parseInt(zIndex) + 1;
			}
			placeholder.css({pointerEvents: 'none',cursor: 'text',display: 'none',zIndex: zIndex, color: color, position: 'absolute', left: left, top: top ,width: width ,height: height });
			
			var value = function(){
				var val = _self.val();
				if(val == typeof(undefined) || val == null || val == '')
				return '';
				else
				return val;
			};
			var addPlaceholder = function(){
				placeholder.css('display','');
			};
			var remPlaceholder = function(){
				placeholder.css('display','none');
			};
			var hasPlaceholder = function(){
				return placeholder.css('display') != 'none';
			};
		
			if(value() == ''){
				addPlaceholder();
			}
			//needs to be keydown to get the value before changed
			_self.focusin(function(e){
				if(hasPlaceholder() == true){
					remPlaceholder();
				}
			});
			_self.focusout(function(e){
				if(value() == ''){
					addPlaceholder();
				}
			});
		}
	});
};
$(document).ready(function(){
	$('input[placeholder][type=text]').placeholder();
});