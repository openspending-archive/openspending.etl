jQuery(function ($) {
  $('a.confirm').click(function () {
     var m = $(this).attr('title');
     if (!m || m == '') {
       m = "Are you sure you want to do this?";
     }
     return confirm(m);
  });
});