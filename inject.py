import re

html = open('index.html').read()

bot = open('bot_snippet.html').read()

result = html.replace('</body>', bot + '</body>')
open('index.html', 'w').write(result)
print("Done! Bot injected into index.html")
