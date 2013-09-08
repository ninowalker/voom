from email.mime.multipart import MIMEMultipart
from voom.codecs import MessageCodec
import email


class MIMEMessageCodec(MessageCodec):
    TYPE = "multipart/mixed"

    def mimetypes(self):
        """Content types handled"""
        return [self.TYPE]

    def encode_message(self, messages, headers):
        """Encodes a list of messages as MIME parts with
        headers included at the root of the document."""
        msg = MIMEMultipart()
        for header in headers:
            msg[header] = headers[header]

        if not isinstance(messages, list):
            messages = [messages]

        for message in messages:
            part = self._encode_part(message)
            msg.attach(part)
        return msg.as_string()

    def decode_message(self, str_or_fp):
        """Converts a string/file object into a set of headers and decoded parts."""
        if hasattr(str_or_fp, 'read'):
            msg = email.message_from_file(str_or_fp)
        else:
            msg = email.message_from_string(str_or_fp)

        parts = []
        for part in msg.get_payload():
            parts.append(self._decode_part(part, part.get_content_type()))

        return dict(msg.items()), parts
    