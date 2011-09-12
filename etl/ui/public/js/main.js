if (typeof(OpenSpending) === "undefined") { OpenSpending = {}; }  

jQuery(function ($) {
  $('a.confirm').click(function () {
    var m = $(this).attr('title');
    if (!m || m == '') {
      m = "Are you sure you want to do this?";
    }
    return confirm(m);
  });
  
  $('.ckan-choose-resource').each(function () {
    $(this).append('<span class="chooser">&mdash; <a href="#">Choose resource</a></span>');
  });
  
  $('.ckan-choose-resource').delegate('.chooser a', 'click', function (e) {
    var root = $(this).parents('.ckan-choose-resource').eq(0);

    pkg = root.data('ckan-package');
    prev = root.data('ckan-prev-resource-id');
    hint = root.data('ckan-hint');
    pkgData = OpenSpending._ckan_package_cache[pkg];
    
    console.log(pkgData);
    
    $(this).replaceWith('<form class="inline" action="/task/add_hint" method="post">' +
                          '<input type="hidden" name="pkg" value="' + pkg + '">' +  
                          '<input type="hidden" name="hint" value="' + hint + '">' + 
                          '<input type="hidden" name="prev_resource_id" value="' + prev + '">' + 
                          '<select name="resource_id"></select>' + 
                          '<input type="submit">' + 
                        '</form>');
                     
    $(pkgData['resources']).each(function () {
      root.find('select').append(
        $('<option value="' + this['id'] + '">' + this['description'] + '</option>')
      );
    });  
    
    e.preventDefault();
  });
});