var lastSeqid = -1;
var currentTopic = {};

function poll(url) {
    var onOff = 'off';
    var data = {"url":url};
    currentTopic = "system";
    $.ajax({
        type: "GET",
        dataType: "json",
        data: data,
        url: url + "/longpoll?topic=" + currentTopic + "&seqid=" + lastSeqid + "&count=5",
        //context: document.body,
        success: function(data, status, request) {
            console.dir(data);
            //alert(data);
            lastSeqid = data.seqid;
            for (var messageIndex in data.message){
                var messageJSON = data.message[messageIndex];
                var message = eval("("+messageJSON+")");
                alert(message.name + ": " + message.message + "\n");
            }
            setTimeout(
                'poll(data.url)', /* Request next message */
                100
            );
        },
        error: function(request, status, error) {
            console.dir(request);
            //_dir(request);
            alert("Error: " + request + " : " + status + " : " + error);
            _log("Error polling: " + error);
            setTimeout(
                'poll(request.url)', /* Request next message */
                5000 /* in 5 seconds */
            );
        }
    });
}


$(document).ready(function() {
    //alert("Starting notifications to: " + notificationSystem.pollsterPollURL)
    //poll();
});
