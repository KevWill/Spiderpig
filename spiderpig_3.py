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
        for i, tweet in enumerate(self.input):
            self.q.put((i, tweet))

    def get_links(self, num_threads = None, verbose = False):
        """
        Main method to get links of input
        :return: domains list, links list, failed list
        """
        threads = []
        num_threads = num_threads or (int(len(self.input) / 4) if (len(self.input) / 4) <= 50 else 50)
        for i in range(num_threads):
            t = threading.Thread(target=self._get_links, args = (verbose,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        domains = [d[1] for d in sorted(self.domains_list)]
        links = [d[1] for d in sorted(self.links_list)]

        return {'domains': domains, 'links': links, 'failed': self.failed_list}

    def _get_links(self, verbose = False):
        while not self.q.empty():
            if verbose:
                q_size = self.q.qsize()
                if q_size < 10:
                    update = True
                elif q_size < 100 and q_size % 10 == 0:
                    update = True
                elif q_size % 100 == 0:
                    update = True
                else:
                    update = False
                if update:
                    print('{} berichten over'.format(q_size))
            item = self.q.get()
            index = item[0]
            tweet = item[1]
            links = self._links_from_tweet(tweet)
            links_not_ignored = []
            for link in links:
                parsed_link = urlparse(link)
                if parsed_link.netloc not in Spiderpig.ignore_domains:
                    links_not_ignored.append(parsed_link)
            tweet_links = []
            tweet_domains = []
            for link in links_not_ignored:
                tweet_links.append(urlunparse(link))
                unparsed = urlunparse([link.scheme, link.netloc, '', '', '', ''])
                tweet_domains.append(unparsed)
            self.links_list.append((index, tweet_links))
            self.domains_list.append((index, tweet_domains))

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
        urls_to_return = []
        for url in urls:
            full_url = url[0]
            redirect = get_redirect(full_url)
            if redirect:
                urls_to_return.append(redirect)
            else:
                urls_to_return.append('')
        return urls_to_return