import sys, os
import requests

BASE = "https://apod.nasa.gov/apod/"
URL = BASE + "ap{:02}{:02}{:02}.html"
API = "https://api.nasa.gov/planetary/apod?api_key={}&start_date={}&end_date={}"
KEY = "vfHq6XWkteywljmTaM8UWz40IAJZERLXqsSsBUqv"

def usage(args):
    prg = os.path.basename(args[0])
    print("python {} yyyy-mm-dd".format(prg))
    exit(1)

def scrape(y, m, d):
    url = URL.format(y % 100, m, d)
    r = requests.get(url)
    prev = None
    for line in r.iter_lines(decode_unicode="utf8"):
        if line.lower().startswith("<img "):
            break
        prev = line
    img = BASE + prev.split('"')[1]
    return img

def api(y, m, d):
    date = "{:04}-{:02}-{:02}".format(y, m, d)
    url = API.format(KEY, date, date)
    r = requests.get(url)
    json = r.json()
    return json[0]["hdurl"]

def main(args):
    if len(args) != 2:
        #usage(args)
        y, m, d = map(int, "2017-04-22".split("-"))
    else:
        try:
            y, m, d = map(int, args[1].split("-"))
        except:
            usage(args)

    # Scrape
    #print(scrape(y, m, d))
    print(api(y, m, d))

if __name__ == "__main__":
    main(sys.argv)
