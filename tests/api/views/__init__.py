from mohawk import Sender


def get_mohawk_sender(url):
    sender = Sender(
        credentials={'id': 'iss1', 'key': 'secret1', 'algorithm': 'sha256'},
        url=url,
        method='GET',
        content='',
        content_type='',
    )
    return sender


def make_hawk_auth_request(client, url):
    sender = get_mohawk_sender('http://localhost:80' + url)
    response = client.get(url, headers={'Authorization': sender.request_header})
    return response
