

// function loadHTMLTable(data){
//     const table = document.querySelector('')
// }

$('#left_nav .leftmenu-inner ul li a').click(function(){
    var _link = $(this).attr('_link');
    $('#main').attr('src', _link);
});

$('#user_collapse ul li a').click(function(){
    var _link = $(this).attr('_link');
    console.log(_link);
    if (_link == "/main.html") {
        window.location.reload();
    }
    $('#main').attr('src', _link);
});

function signOut() {
    var auth2 = gapi.auth2.getAuthInstance();
    auth2.signOut().then(function () {
    console.log('User signed out.');
    });
}