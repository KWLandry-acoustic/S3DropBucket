import http from 'https'; 

const defaultOptions = {
    host: 'https://api-campaign-us-6.goacoustic.com/XMLAPI',
    port: 443,
    headers: {
        'Content-Type': 'application/json',
    }
}
const path: string = "https://api-campaign-us-6.goacoustic.com/XMLAPI"
const payload: string = "" 

const post = (path: string, payload: string) => new Promise((resolve, reject) => {
    const options = { ...defaultOptions, path, method: 'POST' };
    const req = http.request(options, res => {
        let buffer = "";
        res.on('data', chunk => buffer += chunk)
        res.on('end', () => resolve(JSON.parse(buffer)))
    });
    req.on('error', e => reject(e.message));
    req.write(JSON.stringify(payload));
    req.end();
})

