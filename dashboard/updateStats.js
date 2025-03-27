/* UPDATE THESE VALUES TO MATCH YOUR SETUP */

const PROCESSING_STATS_API_URL = "http://acit3855-navdeep.westus.cloudapp.azure.com:8100/stats";
const ANALYZER_API_URL = {
    stats: "http://acit3855-navdeep.westus.cloudapp.azure.com:8200/stats",
    air_all: "http://acit3855-navdeep.westus.cloudapp.azure.com:8200/air",
    traffic_all: "http://acit3855-navdeep.westus.cloudapp.azure.com:8200/traffic"
};

const makeReq = (url, cb) => {
    fetch(url)
        .then(res => {
            if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
            return res.json();
        })
        .then(result => cb(result))
        .catch(err => updateErrorMessages(err.message));
};

const updateCodeDiv = (result, elemId) => {
    const elem = document.getElementById(elemId);
    elem.innerText = typeof result === "string" ? result : JSON.stringify(result, null, 2);
};

const getLocaleDateStr = () => (new Date()).toUTCString();

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr();

    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"));
    makeReq(ANALYZER_API_URL.stats, (result) => updateCodeDiv(result, "analyzer-stats"));

    // Fetch all air events and show a random one
    makeReq(ANALYZER_API_URL.air_all, (airEvents) => {
        if (airEvents.length > 0) {
            const randomAir = airEvents[Math.floor(Math.random() * airEvents.length)];
            updateCodeDiv(randomAir, "event-snow");
        } else {
            updateCodeDiv("No air events available", "event-snow");
        }
    });

    // Fetch all traffic events and show a random one
    makeReq(ANALYZER_API_URL.traffic_all, (trafficEvents) => {
        if (trafficEvents.length > 0) {
            const randomTraffic = trafficEvents[Math.floor(Math.random() * trafficEvents.length)];
            updateCodeDiv(randomTraffic, "event-lift");
        } else {
            updateCodeDiv("No traffic events available", "event-lift");
        }
    });
};

const updateErrorMessages = (message) => {
    const id = Date.now();
    const msg = document.createElement("div");
    msg.id = `error-${id}`;
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`;
    const messages = document.getElementById("messages");
    messages.style.display = "block";
    messages.prepend(msg);
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`);
        if (elem) elem.remove();
    }, 7000);
};

const setup = () => {
    getStats();
    setInterval(getStats, 4000); // Update every 4 seconds
};

document.addEventListener('DOMContentLoaded', setup);
