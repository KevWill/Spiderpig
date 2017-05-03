import threading
from queue import Queue
import requests
import re
from requests.exceptions import ConnectionError, ReadTimeout
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

class Spiderpig():

    ignore_domains = ['twitter.com', 't.co', 'ow.ly', 'bit.ly', 'lnkd.in']

    def __init__(self, input):
        self.failed_list = []
        self.links_list = []
        self.domains_list = []
        self.ignore_domains = []
        self.input = input
        self.q = Queue()
        for tweet in self.input:
            self.q.put(tweet)

    def get_links(self):
        """
        Main method to get links of input
        :return: domains list, links list, failed list
        """
        threads = []
        num_threads = int(len(self.input) / 4) if (len(self.input) / 4) <= 100 else 100
        for i in range(num_threads):
            t = threading.Thread(target=self._get_links)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        return self.domains_list, self.links_list, self.failed_list

    def _get_links(self, domain_only = False):
        while not self.q.empty():
            item = self.q.get()
            links = self._links_from_tweet(item)
            links_not_ignored = [urlparse(link) for link in links if urlparse(link).netloc not in self.ignore_domains]
            self.links_list.append([urlunparse(parsed_link) for parsed_link in links_not_ignored])
            self.domains_list.append([urlunparse([parsed_url.scheme, parsed_url.netloc, '', '', '', ''])
                                      for parsed_url in links_not_ignored])
            for link in links:
                parsed_url = urlparse(link)
                if parsed_url.netloc not in self.ignore_domains:
                    self.links_list.append(link)
                    unparsed = urlunparse([parsed_url.scheme, parsed_url.netloc, '', '', '', ''])
                    self.domains_list.append(unparsed)

    def _links_from_tweet(self, tweet_text):
        def get_redirect(url):
            try:
                r = requests.head(url, timeout=8)
            except (ConnectionError, ReadTimeout):
                self.failed_list.append(url)
                return None
            try:
                r = requests.head(r.headers['location'], timeout=8)
                status_code = r.status_code
            except (Exception, ConnectionError, ReadTimeout):
                if r.status_code == 200:
                    return r.url
                elif r.status_code < 400:
                    return r.headers['location']
                else:
                    self.failed_list.append(url)
                    return None

            if 300 <= status_code < 400:
                loops = 0
                while 300 <= r.status_code < 400 and loops < 10:
                    loops += 1
                    try:
                        previous = r.url
                        redirect_location = r.headers['location']
                        r = requests.head(redirect_location, timeout=8)
                        previous = r.url
                    except requests.exceptions.MissingSchema:
                        parse_previous = urlparse(previous)
                        domain = '{}://{}'.format(parse_previous.scheme, parse_previous.netloc)
                        try:
                            r = requests.head(domain + redirect_location, timeout=8)
                            previous = r.url
                        except (Exception, ConnectionError, ReadTimeout):
                            try:
                                r = requests.get(url, timeout=8)
                            except (Exception, ConnectionError, ReadTimeout):
                                return r.headers['location']
                                # failed_list.append(url)
                                # return None
                    except (Exception, ConnectionError, ReadTimeout):
                        try:
                            r = requests.get(url, timeout=8)
                        except (Exception, ConnectionError, ReadTimeout):
                            return r.headers['location']
                            # failed_list.append(url)
                            # return None
            parsed_url = urlparse(r.url)
            qd = parse_qs(parsed_url.query, keep_blank_values=True)
            filtered = dict( (k, v) for k, v in qd.items() if not k.startswith('utm_'))
            url_without_args = urlunparse([
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                urlencode(filtered, doseq=True),
                parsed_url.fragment
            ])
            return url_without_args
        regex_url = re.compile('((http|ftp|https):\/\/([\w\-_]+(?:(?:\.[\w\-_]+)+))([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?)')
        urls = re.findall(regex_url, tweet_text)
        for url in urls:
            full_url = url[0]
            redirect = get_redirect(full_url)
            if redirect:
                yield redirect
            else:
                yield ''