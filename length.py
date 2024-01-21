import asyncio
import csv
from pyppeteer import launch

async def periodic_writer(batch_results, output_file, batch_size):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["URL", "Height in Pixels"])

    while True:
        if len(batch_results) >= batch_size:
            with open(output_file, 'a', newline='') as f:
                writer = csv.writer(f)
                for _ in range(min(batch_size, len(batch_results))):
                    result = batch_results.pop(0)
                    writer.writerow(result)

        await asyncio.sleep(5)  # Sleep for 5 seconds before checking again

async def worker(name, queue, batch_results):
    try:
        browser = await launch({
            'headless': True,
            'args': ['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security']
        })

        while not queue.empty():
            url = await queue.get()
            try:
                page = await browser.newPage()
                await page.setViewport({'width': 1200, 'height': 800})
                response = await page.goto(url)
                content_type = response.headers.get('content-type', '')

                if 'text/html' not in content_type:
                    print(f"[Worker {name}] Skipping {url} due to non-HTML content type: {content_type}")
                    continue

                dimensions = await page.evaluate('''() => {
                    return {
                        height: Math.max(
                            document.body.scrollHeight, 
                            document.body.offsetHeight,
                            document.documentElement.clientHeight, 
                            document.documentElement.scrollHeight, 
                            document.documentElement.offsetHeight
                        )
                    }
                }''')

                print(f"[Worker {name}] Crawled {url}, Height: {dimensions['height']}")
                batch_results.append((url, dimensions['height']))

            except Exception as e:
                print(f"[Worker {name}] Error accessing {url}. Error: {e}")

            finally:
                await page.close()

    except Exception as e:
        print(f"[Worker {name}] Browser launch or operation failed. Error: {e}")

    finally:
        await browser.close()

async def main():
    input_file = "/PATH-TO-FILE.csv"
    output_file = "/PATH-TO-FILE.csv"
    num_workers = 25
    batch_size = 50

    with open(input_file, 'r') as f:
        reader = csv.reader(f)
        urls = [row[0] for row in reader]

    queue = asyncio.Queue()
    for url in urls:
        queue.put_nowait(url)

    batch_results = []
    tasks = []

    # Add periodic_writer task
    tasks.append(asyncio.ensure_future(periodic_writer(batch_results, output_file, batch_size)))

    for i in range(num_workers):
        task = asyncio.ensure_future(worker(f"Worker-{i+1}", queue, batch_results))
        tasks.append(task)

    await asyncio.gather(*tasks)

    # Ensure remaining results are written
    await periodic_writer(batch_results, output_file, 0)

    print("Crawl complete.")

if __name__ == "__main__":
    asyncio.run(main())
