import requests
import bs4


def get_vacancy(url: str):
    print(f"get_vacancy: {url}")
    result = {
        "link": url
    }
    vacancy = requests.get(url)
    soup = bs4.BeautifulSoup(vacancy.text, "html.parser")
    # print(soup)
    result["position_name"] = soup.find_all('h1', class_="top-card-layout__title")[0].getText()
    result["company_name"] = cut(soup.find_all('a', class_="topcard__org-name-link")[0].get_text())
    description = soup.find_all('div', class_="show-more-less-html__markup")[0]
    description = description.contents
    description = "\n".join(list(map(lambda description_row: parse_description(description_row), description)))
    result["description"] = description
    return result


def parse_description(description_row):
    if type(description_row) == bs4.element.NavigableString:
        if description_row == '\n':
            return ''
        return description_row + '\n'
    else:
        content = description_row.contents
        if len(content) == 1:
            return content[0].get_text()
        else:
            content = list(map(lambda content_row: " - " + str(content_row.get_text()), content))
            return "\n".join(content)


def cut(company_name: str):
    return company_name.replace('\n', "").strip()

