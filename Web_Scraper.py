import requests
from bs4 import BeautifulSoup

def scrape_article_content(url):
    try:
        # Send HTTP GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise exception if status is not 200
        html = response.text

        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        # The following logic is customized for Washington Post articles.
        # Article content is typically within <article> tags.
        article = soup.find('article')
        if not article:
            # Sometimes, the content is in divs with specific attributes
            possible = soup.find('div', {'data-qa': 'headline'})
            article_text = possible.get_text(separator='\n', strip=True) if possible else None
            if article_text:
                return article_text
            return "Article content could not be found."

        # Main paragraphs are usually <p> tags inside <article>
        paragraphs = article.find_all('p')
        text_content = []
        for p in paragraphs:
            text = p.get_text(separator=' ', strip=True)
            if text:
                text_content.append(text)
        article_body = "\n\n".join(text_content)
        if not article_body.strip():
            return "No article content extracted."
        return article_body
    except Exception as e:
        return f"An error occurred: {e}"

# Example usage:
if __name__ == "__main__":
    url = "https://edition.cnn.com/travel/article/scenic-airport-landings-2020/index.html"
    content = scrape_article_content(url)
    print(content)
