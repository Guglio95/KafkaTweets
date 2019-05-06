var baseWS = window.location.host; //  example: 127.0.0.1:4567
var baseURL = "";// example: http://127.0.0.1:4567

var filter, query;
var isLiveMode = false;
var isNoTweetsToShow = false;
var isLoading = true;
var ws;

//Adds an array of tweets to the dom
function addTweets(tweets, append = true) {

    if (tweets.length == 0 && isLoading) {
        $("#tweetsContainer").html("<center><i>There are no tweets to show at the moment.</i></center>");
        isNoTweetsToShow = true;
        isLoading = false;
        return;
    }
    if (tweets.length > 0 && isLoading) {
        //If we have tweets to show, remove "is loading" banner.
        $("#tweetsContainer").html("");
        isLoading = false;
        isNoTweetsToShow = false;
    }

    tweets.forEach(function(item, index) {
        addTweet(item, append);
    });
}

//Adds a single tweet to the dom
function addTweet(tweet, append = true) {
    var template = $('#hidden-template').html();
    //Clone the template
    var item = $(template).clone();

    //Fill required fields
    $(item).find('#tweetAuthor').append(tweet.author);
    $(item).find('#tweetLocation').append("<a class='setLocation' href='#'>" + tweet.location + "</a>");
    $(item).find('#tweetContent').append(tweet.content);
    $(item).find('#tweetTimestamp').append(unixtimeToString(tweet.timestamp));

    //Fill optional fields
    if (tweet.tags.length > 0) {
        for (var i = 0; i < tweet.tags.length; i++) {
            $(item).find('#tweetTags').append("<a class='setTag' href='#'>" + tweet.tags[i] + "</a> ");
        }
    } else {
        $(item).find('#tweetTags').hide(); //Hide the field if empty
    }

    //Fill optional fields
    if (tweet.mentions.length > 0) {
        for (var i = 0; i < tweet.mentions.length; i++) {
            $(item).find('#tweetMentions').append("<a class='setMention' href='#'>" + tweet.mentions[i] + "</a> ");
        }
    } else {
        $(item).find('#tweetMentions').hide(); //Hide the field if empty
    }

    //Add to the source
    if (append) {
        $('#tweetsContainer').append(item);
    } else {
        $('#tweetsContainer').prepend(item);
    }
}

//Converts unixtime to human readable string.
function unixtimeToString(unixtime) {
    var date = new Date(unixtime * 1000);
    return date.toLocaleDateString() + " " + date.toLocaleTimeString();
}

//Batch downloads sliding windows from server
function pollServer() {
    $("#tweetsContainer").html("<center><i>Loading...</i></center>");
    isLoading = true;
    isNoTweetsToShow = false;
    $.get(baseURL + "/tweets/" + filter + "/" + query + "/latest", function(data) {
        addTweets(data);
    }, "json");
}

//Callback function used when a button on the search form is pressed.
function searchButton(filter) {
    var query = $("#searchQuery").val(); //Gets the query string
    if (query == "") {
        alert("Please provide a query");
        return;
    }
    setFilterAndQuery(filter, query);
}

function setFilterAndQuery(filter, query) {
    window.filter = filter;
    window.query = query;
    $("#tweetsShowing").html("Filtering by <i>" + filter + "</i> with query <i>" + query + "</i>");

    //If the user wanted to use WS update the preference with the server.
    if (!isLiveMode) {
        pollServer(); //if user wanted to manually download tweets, download them all for him.
    } else {
        $("#tweetsContainer").html("<center><i>Loading...</i></center>");
        isLoading = true;
        isNoTweetsToShow = false;
        ws.send(filter + "/" + query); // Send to server via WS a message
    }
}



$(document).ready(function() {
    $('[data-role="tags-input"]').tagsInput(); //Init of hashtag fields.

    ws = new WebSocket("ws://"+baseWS+"/tweets/ws");
    ws.onmessage = function(event) {
        //Array of tweets is parsed from json and reversed since every bunch of tweets we receive
        //is ordered starting from the newest one.
        $("#tweetsContainer").html("");
        addTweets(JSON.parse(event.data).reverse(), false);
    }

    $("#postTweet").submit(function(event) {
        event.preventDefault();
        var author = $("#author").val();
        var location = $("#location").val();
        var content = $("#content").val();
        var tags = $("#tags").val();
        var mentions = $("#mentions").val();

        $.post(baseURL + "/tweets", {
                'author': author,
                'location': location,
                'content': content,
                'timestamp': Math.floor(Date.now() / 1000),
                'tags': tags,
                'mentions': mentions
            })
            .fail(function(data) {
                alert("Connection error");
            })
            .done(function(data) {
                alert("Posted");
            });
    });

    //Callback on Search buttons
    $("#searchMention").click(function(event) {
        searchButton("mention");
    });
    $("#searchLocation").click(function(event) {
        searchButton("location");
    });
    $("#searchHashtag").click(function(event) {
        searchButton("tag");
    });
    $("#sync").click(function(event) {
        pollServer();
    });


    //Buttons to enable / disable live mode.
    $("#enableLiveUpdate").click(function(event) {
        isLiveMode = true;
        $("#enableLiveUpdate").hide();
        $("#disableLiveUpdate").show();
        if (filter !== undefined && query !== undefined) setFilterAndQuery(filter, query); //Run again request with new fetching method
    });
    $("#disableLiveUpdate").hide();

    $("#disableLiveUpdate").click(function(event) {
        isLiveMode = false;
        $("#enableLiveUpdate").show();
        $("#disableLiveUpdate").hide();
        if (filter !== undefined && query !== undefined) setFilterAndQuery(filter, query); //Run again request with new fetching method
    });


    //Callback on tweets links
    $("#tweetsContainer").on('click', '.setMention', function() {
        setFilterAndQuery("mention", this.innerHTML);
    });
    $("#tweetsContainer").on('click', '.setLocation', function() {
        setFilterAndQuery("location", this.innerHTML);
    });
    $("#tweetsContainer").on('click', '.setTag', function() {
        setFilterAndQuery("tag", this.innerHTML);
    });
});
