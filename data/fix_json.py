import re

with open('new_sulseltenggara.json', encoding='utf-8') as f:
    content = f.read()


content = re.sub(r'(\})\s*\n\s*(\{)', r'\1,\n\2', content)


new_content = '[\n' + content.strip() + '\n]'

with open('new_new_sulseltenggara.json', 'w', encoding='utf-8') as f:
    f.write(new_content)

print('✅ Berhasil! File baru: new_new_sulseltenggara.json')
