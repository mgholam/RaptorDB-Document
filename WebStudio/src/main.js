import App from './App.svelte';

const app = new App({
    target: document.body,
});

window.ServerURL = document.location.protocol + "//" + document.location.hostname + ":" + document.location.port + "/";
window.GET = function(url, callback, error) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', window.ServerURL + url);

    xhr.onreadystatechange = function() {
        if (xhr.readyState === 1) { return; } else if (xhr.readyState === 4) {
            if (xhr.status < 400) {
                var data = null;
                if (xhr.responseText !== "")
                    data = JSON.parse(xhr.responseText);
                else
                    data = "";
                if (callback)
                    callback(data);
            } else if (xhr.status === 401) {
                xhr.abort();
                if (callback)
                    callback("");
            } else if (error !== null)
                error(xhr.responseText);
        }
    };
    xhr.onerror = function(err) {
        // $.showDialog("Sorry", "<p>Connection Failed!</p>");
        console.log(err);
    };
    xhr.withCredentials = false;
    xhr.send();
}
window.LOAD = function(url, callback, error) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', window.ServerURL + url);

    xhr.onreadystatechange = function() {
        if (xhr.readyState === 1) { return; } else if (xhr.readyState === 4) {
            // console.log(xhr);
            if (xhr.status < 400) {
                var data = null;
                if (xhr.responseText !== "")
                    data = xhr.responseText;
                else
                    data = "";
                if (callback)
                    callback(data);
            } else if (xhr.status === 401) {
                xhr.abort();
                if (callback)
                    callback("");
            } else if (error !== null)
                error(xhr.responseText);
        }
    };
    xhr.onerror = function(err) {
        // $.showDialog("Sorry", "<p>Connection Failed!</p>");
        console.log(err);
    };
    xhr.withCredentials = false;
    xhr.send();
}

export default app;