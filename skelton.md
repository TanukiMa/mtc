### 下記方針に沿って、各 pythonのcodeを提案して ### github runners上でwebクローラー+日本語形態素解析して、新語を探す作業をしたい。可能かな？ 形態素解析には Javaで書かれたSudachiを使いたい。Github runnersには Ubuntuを使いたい。対象は厚労省のサイト。専門用語。既存辞書と照合。実行頻度は定期実行。医療用語や用語の保存や呼び出しには外部sql db(supabase)を使うのはどうか

```yaml
name: Medical Term Crawler and Analysis

on:
  schedule:
    - cron: '0 2 * * *'  # 毎日午前2時に実行
  workflow_dispatch:

jobs:
  crawl-and-analyze:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      
    - name: Set up JDK 11
      uses: actions/setup-java@v4
      with:
        java-version: '11'
        distribution: 'temurin'
        
    - name: Install dependencies
      run: |
        sudo apt-get update
        sudo apt-get install -y wget curl maven
        
    - name: Download Sudachi
      run: |
        wget https://github.com/WorksApplications/Sudachi/releases/download/v0.7.3/sudachi-0.7.3-executable.jar
        mkdir -p sudachi
        mv sudachi-0.7.3-executable.jar sudachi/
        
    - name: Download Sudachi dictionaries
      run: |
        cd sudachi
        wget https://github.com/WorksApplications/SudachiDict/releases/download/v20231110/sudachi-dictionary-20231110-full.zip
        unzip sudachi-dictionary-20231110-full.zip
        
    - name: Create Sudachi config
      run: |
        cat > sudachi/sudachi.json << 'EOF'
        {
          "systemDict": "system_full.dic",
          "characterDefinitionFile": "char.def",
          "inputTextPlugin": [
            {
              "class": "com.worksap.nlp.sudachi.DefaultInputTextPlugin"
            }
          ],
          "oovProviderPlugin": [
            {
              "class": "com.worksap.nlp.sudachi.MeCabOovProviderPlugin",
              "charDef": "char.def",
              "unkDef": "unk.def"
            }
          ],
          "pathRewritePlugin": [
            {
              "class": "com.worksap.nlp.sudachi.JoinNumericPlugin"
            },
            {
              "class": "com.worksap.nlp.sudachi.JoinKatakanaOovPlugin"
            }
          ]
        }
        EOF
        
    - name: Create web crawler
      run: |
        cat > WebCrawler.java << 'EOF'
        import java.io.*;
        import java.net.*;
        import java.util.*;
        import java.util.regex.*;
        import java.nio.file.*;

        public class WebCrawler {
            private static final String BASE_URL = "https://www.mhlw.go.jp";
            private static final String[] TARGET_PATHS = {
                "/stf/seisakunitsuite/bunya/kenkou_iryou/",
                "/stf/seisakunitsuite/bunya/0000161801.html",
                "/stf/newpage_",
                "/content/000"
            };
            
            public static void main(String[] args) throws IOException {
                Set<String> crawledUrls = new HashSet<>();
                List<String> textContent = new ArrayList<>();
                
                for (String path : TARGET_PATHS) {
                    crawlPath(BASE_URL + path, crawledUrls, textContent, 0, 2);
                }
                
                // テキストをファイルに保存
                Files.write(Paths.get("crawled_content.txt"), 
                           String.join("\n", textContent).getBytes("UTF-8"));
                
                System.out.println("Crawled " + textContent.size() + " pages");
            }
            
            private static void crawlPath(String url, Set<String> crawled, 
                                        List<String> content, int depth, int maxDepth) {
                if (depth > maxDepth || crawled.contains(url)) return;
                
                try {
                    crawled.add(url);
                    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                    conn.setRequestProperty("User-Agent", "Mozilla/5.0");
                    conn.setConnectTimeout(5000);
                    conn.setReadTimeout(10000);
                    
                    if (conn.getResponseCode() == 200) {
                        String html = readResponse(conn);
                        String text = extractText(html);
                        if (!text.trim().isEmpty()) {
                            content.add(text);
                        }
                        
                        // リンクを抽出して再帰的にクロール
                        extractLinks(html, url).stream()
                            .filter(link -> link.contains("mhlw.go.jp"))
                            .forEach(link -> crawlPath(link, crawled, content, depth + 1, maxDepth));
                    }
                    
                    Thread.sleep(1000); // レート制限
                } catch (Exception e) {
                    System.err.println("Error crawling " + url + ": " + e.getMessage());
                }
            }
            
            private static String readResponse(HttpURLConnection conn) throws IOException {
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(), "UTF-8"));
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
                reader.close();
                return content.toString();
            }
            
            private static String extractText(String html) {
                return html.replaceAll("<[^>]+>", " ")
                          .replaceAll("\\s+", " ")
                          .trim();
            }
            
            private static List<String> extractLinks(String html, String baseUrl) {
                List<String> links = new ArrayList<>();
                Pattern pattern = Pattern.compile("href=[\"']([^\"']+)[\"']");
                Matcher matcher = pattern.matcher(html);
                
                while (matcher.find()) {
                    String link = matcher.group(1);
                    if (link.startsWith("/")) {
                        link = "https://www.mhlw.go.jp" + link;
                    } else if (!link.startsWith("http")) {
                        continue;
                    }
                    links.add(link);
                }
                return links;
            }
        }
        EOF
        
    - name: Create morphological analyzer
      run: |
        cat > MorphologicalAnalyzer.java << 'EOF'
        import java.io.*;
        import java.nio.file.*;
        import java.util.*;
        import java.util.stream.*;

        public class MorphologicalAnalyzer {
            public static void main(String[] args) throws IOException {
                String content = Files.readString(Paths.get("crawled_content.txt"));
                List<String> sentences = Arrays.asList(content.split("[。！？\n]"));
                
                Set<String> newTerms = new HashSet<>();
                
                for (String sentence : sentences) {
                    if (sentence.trim().isEmpty()) continue;
                    
                    // Sudachiで形態素解析
                    List<String> terms = analyzeSentence(sentence.trim());
                    
                    // 専門用語らしい語を抽出
                    terms.stream()
                        .filter(this::isMedicalTerm)
                        .forEach(newTerms::add);
                }
                
                // 新語をファイルに保存
                Files.write(Paths.get("new_terms.txt"), 
                           newTerms.stream().sorted().collect(Collectors.toList()));
                
                System.out.println("Found " + newTerms.size() + " potential new terms");
            }
            
            private static List<String> analyzeSentence(String sentence) {
                List<String> terms = new ArrayList<>();
                
                try {
                    ProcessBuilder pb = new ProcessBuilder(
                        "java", "-jar", "sudachi/sudachi-0.7.3-executable.jar",
                        "-r", "sudachi/sudachi.json", "-m", "A"
                    );
                    Process process = pb.start();
                    
                    PrintWriter writer = new PrintWriter(process.getOutputStream());
                    writer.println(sentence);
                    writer.close();
                    
                    BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()));
                    String line;
                    
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 2) {
                            String surface = parts[0];
                            String pos = parts[1];
                            
                            // 名詞、形容詞、動詞などをフィルタ
                            if (pos.startsWith("名詞") || pos.startsWith("形容詞") || 
                                pos.startsWith("動詞")) {
                                if (surface.length() >= 2) {
                                    terms.add(surface);
                                }
                            }
                        }
                    }
                    
                    process.waitFor();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                return terms;
            }
            
            private boolean isMedicalTerm(String term) {
                // 医療関連用語の判定ロジック
                String[] medicalKeywords = {
                    "症", "病", "療", "治", "診", "薬", "検", "査", "手", "術",
                    "ウイルス", "細菌", "感染", "予防", "接種", "ワクチン",
                    "健康", "医療", "患者", "医師", "看護", "病院", "診療"
                };
                
                for (String keyword : medicalKeywords) {
                    if (term.contains(keyword)) {
                        return true;
                    }
                }
                
                // カタカナ4文字以上も医療用語の可能性
                return term.matches("^[ァ-ヶー]{4,}$");
            }
        }
        EOF
        
    - name: Create database handler
      run: |
        cat > DatabaseHandler.java << 'EOF'
        import java.io.*;
        import java.net.http.*;
        import java.net.URI;
        import java.nio.file.*;
        import java.util.*;
        import java.util.stream.*;
        import java.time.LocalDateTime;

        public class DatabaseHandler {
            private static final String SUPABASE_URL = System.getenv("SUPABASE_URL");
            private static final String SUPABASE_KEY = System.getenv("SUPABASE_ANON_KEY");
            
            public static void main(String[] args) throws IOException, InterruptedException {
                List<String> newTerms = Files.readAllLines(Paths.get("new_terms.txt"));
                
                // 既存用語を取得
                Set<String> existingTerms = getExistingTerms();
                
                // 新語をフィルタリング
                List<String> actualNewTerms = newTerms.stream()
                    .filter(term -> !existingTerms.contains(term))
                    .collect(Collectors.toList());
                
                // 新語をデータベースに保存
                if (!actualNewTerms.isEmpty()) {
                    saveNewTerms(actualNewTerms);
                    System.out.println("Saved " + actualNewTerms.size() + " new terms");
                } else {
                    System.out.println("No new terms found");
                }
            }
            
            private static Set<String> getExistingTerms() throws IOException, InterruptedException {
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(SUPABASE_URL + "/rest/v1/medical_terms?select=term"))
                    .header("apikey", SUPABASE_KEY)
                    .header("Authorization", "Bearer " + SUPABASE_KEY)
                    .GET()
                    .build();
                
                HttpResponse<String> response = client.send(request, 
                    HttpResponse.BodyHandlers.ofString());
                
                Set<String> terms = new HashSet<>();
                if (response.statusCode() == 200) {
                    String json = response.body();
                    // 簡単なJSON解析（実際のプロジェクトではJSON libraryを使用）
                    String[] parts = json.split("\"term\":");
                    for (int i = 1; i < parts.length; i++) {
                        String term = parts[i].split("\"")[1];
                        terms.add(term);
                    }
                }
                
                return terms;
            }
            
            private static void saveNewTerms(List<String> terms) throws IOException, InterruptedException {
                HttpClient client = HttpClient.newHttpClient();
                
                for (String term : terms) {
                    String json = String.format(
                        "{\"term\":\"%s\",\"source\":\"mhlw\",\"discovered_at\":\"%s\"}",
                        term, LocalDateTime.now().toString()
                    );
                    
                    HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(SUPABASE_URL + "/rest/v1/medical_terms"))
                        .header("apikey", SUPABASE_KEY)
                        .header("Authorization", "Bearer " + SUPABASE_KEY)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
                    
                    HttpResponse<String> response = client.send(request, 
                        HttpResponse.BodyHandlers.ofString());
                    
                    if (response.statusCode() != 201) {
                        System.err.println("Failed to save term: " + term);
                    }
                    
                    Thread.sleep(100); // API rate limiting
                }
            }
        }
        EOF
        
    - name: Compile Java files
      run: |
        javac WebCrawler.java
        javac MorphologicalAnalyzer.java
        javac DatabaseHandler.java
        
    - name: Run web crawler
      run: java WebCrawler
      
    - name: Run morphological analysis
      run: java MorphologicalAnalyzer
      
    - name: Update database
      run: java DatabaseHandler
      env:
        SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
        SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
        
    - name: Upload artifacts
      uses: actions/upload-artifact@v4
      with:
        name: analysis-results
        path: |
          crawled_content.txt
          new_terms.txt
```

```sql
-- Supabaseテーブル作成SQL
CREATE TABLE medical_terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR(255) NOT NULL UNIQUE,
    source VARCHAR(100) NOT NULL,
    discovered_at TIMESTAMP DEFAULT NOW(),
    verified BOOLEAN DEFAULT FALSE,
    category VARCHAR(100),
    definition TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_medical_terms_term ON medical_terms(term);
CREATE INDEX idx_medical_terms_source ON medical_terms(source);
CREATE INDEX idx_medical_terms_discovered_at ON medical_terms(discovered_at);
```

### 一度解析したURLはpassし、HTMLだけではなく、pptx pdf docxも対象とし、並列処理可能にしたい。pip install supabase することで、supabaseと直接やり取りするのがスマートではないか？


```yaml
name: Advanced Medical Term Crawler and Analysis

on:
  schedule:
    - cron: '0 2 * * *'  # 毎日午前2時に実行
  workflow_dispatch:

jobs:
  crawl-and-analyze:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
        
    - name: Set up JDK 11
      uses: actions/setup-java@v4
      with:
        java-version: '11'
        distribution: 'temurin'
        
    - name: Install Python dependencies
      run: |
        pip install --upgrade pip
        pip install supabase requests beautifulsoup4 python-docx python-pptx PyPDF2 concurrent.futures asyncio aiohttp
        
    - name: Install system dependencies
      run: |
        sudo apt-get update
        sudo apt-get install -y wget curl maven libreoffice
        
    - name: Download Sudachi
      run: |
        wget https://github.com/WorksApplications/Sudachi/releases/download/v0.7.3/sudachi-0.7.3-executable.jar
        mkdir -p sudachi
        mv sudachi-0.7.3-executable.jar sudachi/
        
    - name: Download Sudachi dictionaries
      run: |
        cd sudachi
        wget https://github.com/WorksApplications/SudachiDict/releases/download/v20231110/sudachi-dictionary-20231110-full.zip
        unzip sudachi-dictionary-20231110-full.zip
        
    - name: Create Sudachi config
      run: |
        cat > sudachi/sudachi.json << 'EOF'
        {
          "systemDict": "system_full.dic",
          "characterDefinitionFile": "char.def",
          "inputTextPlugin": [
            {
              "class": "com.worksap.nlp.sudachi.DefaultInputTextPlugin"
            }
          ],
          "oovProviderPlugin": [
            {
              "class": "com.worksap.nlp.sudachi.MeCabOovProviderPlugin",
              "charDef": "char.def",
              "unkDef": "unk.def"
            }
          ],
          "pathRewritePlugin": [
            {
              "class": "com.worksap.nlp.sudachi.JoinNumericPlugin"
            },
            {
              "class": "com.worksap.nlp.sudachi.JoinKatakanaOovPlugin"
            }
          ]
        }
        EOF
        
    - name: Create web crawler with document support
      run: |
        cat > crawler.py << 'EOF'
        import asyncio
        import aiohttp
        import requests
        import hashlib
        import os
        import tempfile
        from urllib.parse import urljoin, urlparse
        from bs4 import BeautifulSoup
        from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
        from supabase import create_client, Client
        import PyPDF2
        from docx import Document
        from pptx import Presentation
        import subprocess
        import time
        from typing import Set, List, Dict, Tuple
        import json

        class DocumentCrawler:
            def __init__(self):
                self.supabase: Client = create_client(
                    os.getenv("SUPABASE_URL"),
                    os.getenv("SUPABASE_ANON_KEY")
                )
                self.crawled_urls: Set[str] = self.load_crawled_urls()
                self.session = None
                
            def load_crawled_urls(self) -> Set[str]:
                """既にクロールしたURLをデータベースから取得"""
                try:
                    result = self.supabase.table('crawled_urls').select('url').execute()
                    return {row['url'] for row in result.data}
                except Exception as e:
                    print(f"Error loading crawled URLs: {e}")
                    return set()
            
            def save_crawled_url(self, url: str, content_hash: str):
                """クロールしたURLを保存"""
                try:
                    self.supabase.table('crawled_urls').insert({
                        'url': url,
                        'content_hash': content_hash,
                        'crawled_at': 'now()'
                    }).execute()
                    self.crawled_urls.add(url)
                except Exception as e:
                    print(f"Error saving crawled URL {url}: {e}")
            
            async def crawl_site(self):
                """非同期でサイトをクロール"""
                base_urls = [
                    "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/",
                    "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000161801.html"
                ]
                
                connector = aiohttp.TCPConnector(limit=10)
                timeout = aiohttp.ClientTimeout(total=30)
                
                async with aiohttp.ClientSession(
                    connector=connector, 
                    timeout=timeout,
                    headers={'User-Agent': 'Mozilla/5.0 (compatible; MedicalTermCrawler/1.0)'}
                ) as session:
                    self.session = session
                    
                    tasks = []
                    for base_url in base_urls:
                        task = asyncio.create_task(
                            self.crawl_recursive(base_url, max_depth=3, current_depth=0)
                        )
                        tasks.append(task)
                    
                    await asyncio.gather(*tasks, return_exceptions=True)
            
            async def crawl_recursive(self, url: str, max_depth: int, current_depth: int):
                """再帰的にURLをクロール"""
                if current_depth > max_depth or url in self.crawled_urls:
                    return
                
                if not self.is_target_domain(url):
                    return
                
                try:
                    await self.process_url(url)
                    
                    if current_depth < max_depth:
                        links = await self.extract_links(url)
                        
                        # 並列処理でリンクをクロール
                        semaphore = asyncio.Semaphore(5)  # 同時実行数制限
                        tasks = []
                        
                        for link in links[:20]:  # リンク数制限
                            task = asyncio.create_task(
                                self.crawl_with_semaphore(
                                    semaphore, link, max_depth, current_depth + 1
                                )
                            )
                            tasks.append(task)
                        
                        if tasks:
                            await asyncio.gather(*tasks, return_exceptions=True)
                
                except Exception as e:
                    print(f"Error crawling {url}: {e}")
                
                await asyncio.sleep(0.5)  # レート制限
            
            async def crawl_with_semaphore(self, semaphore, url, max_depth, current_depth):
                async with semaphore:
                    await self.crawl_recursive(url, max_depth, current_depth)
            
            def is_target_domain(self, url: str) -> bool:
                return 'mhlw.go.jp' in url
            
            async def process_url(self, url: str):
                """URLを処理してコンテンツを抽出"""
                try:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return
                        
                        content_type = response.headers.get('content-type', '').lower()
                        content = await response.read()
                        content_hash = hashlib.md5(content).hexdigest()
                        
                        text_content = ""
                        
                        if 'text/html' in content_type:
                            text_content = await self.extract_html_text(content)
                        elif 'application/pdf' in content_type:
                            text_content = await self.extract_pdf_text(content, url)
                        elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
                            text_content = await self.extract_docx_text(content, url)
                        elif 'application/vnd.openxmlformats-officedocument.presentationml.presentation' in content_type:
                            text_content = await self.extract_pptx_text(content, url)
                        elif 'application/vnd.ms-powerpoint' in content_type:
                            text_content = await self.extract_ppt_text(content, url)
                        
                        if text_content and len(text_content.strip()) > 100:
                            await self.save_content(url, text_content, content_hash)
                            self.save_crawled_url(url, content_hash)
                
                except Exception as e:
                    print(f"Error processing {url}: {e}")
            
            async def extract_html_text(self, content: bytes) -> str:
                """HTMLからテキストを抽出"""
                try:
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # 不要なタグを削除
                    for tag in soup(['script', 'style', 'nav', 'header', 'footer']):
                        tag.decompose()
                    
                    text = soup.get_text(separator=' ', strip=True)
                    return ' '.join(text.split())
                except Exception as e:
                    print(f"Error extracting HTML text: {e}")
                    return ""
            
            async def extract_pdf_text(self, content: bytes, url: str) -> str:
                """PDFからテキストを抽出"""
                def extract_sync():
                    try:
                        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                            tmp_file.write(content)
                            tmp_file.flush()
                            
                            text = ""
                            with open(tmp_file.name, 'rb') as file:
                                reader = PyPDF2.PdfReader(file)
                                for page in reader.pages:
                                    text += page.extract_text() + "\n"
                            
                            os.unlink(tmp_file.name)
                            return text
                    except Exception as e:
                        print(f"Error extracting PDF text from {url}: {e}")
                        return ""
                
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    return await loop.run_in_executor(executor, extract_sync)
            
            async def extract_docx_text(self, content: bytes, url: str) -> str:
                """DOCXからテキストを抽出"""
                def extract_sync():
                    try:
                        with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as tmp_file:
                            tmp_file.write(content)
                            tmp_file.flush()
                            
                            doc = Document(tmp_file.name)
                            text = "\n".join([paragraph.text for paragraph in doc.paragraphs])
                            
                            os.unlink(tmp_file.name)
                            return text
                    except Exception as e:
                        print(f"Error extracting DOCX text from {url}: {e}")
                        return ""
                
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    return await loop.run_in_executor(executor, extract_sync)
            
            async def extract_pptx_text(self, content: bytes, url: str) -> str:
                """PPTXからテキストを抽出"""
                def extract_sync():
                    try:
                        with tempfile.NamedTemporaryFile(suffix='.pptx', delete=False) as tmp_file:
                            tmp_file.write(content)
                            tmp_file.flush()
                            
                            prs = Presentation(tmp_file.name)
                            text = ""
                            
                            for slide in prs.slides:
                                for shape in slide.shapes:
                                    if hasattr(shape, "text"):
                                        text += shape.text + "\n"
                            
                            os.unlink(tmp_file.name)
                            return text
                    except Exception as e:
                        print(f"Error extracting PPTX text from {url}: {e}")
                        return ""
                
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    return await loop.run_in_executor(executor, extract_sync)
            
            async def extract_ppt_text(self, content: bytes, url: str) -> str:
                """PPTからテキストを抽出（LibreOfficeを使用）"""
                def extract_sync():
                    try:
                        with tempfile.NamedTemporaryFile(suffix='.ppt', delete=False) as tmp_file:
                            tmp_file.write(content)
                            tmp_file.flush()
                            
                            # LibreOfficeでテキストに変換
                            output_dir = tempfile.mkdtemp()
                            subprocess.run([
                                'libreoffice', '--headless', '--convert-to', 'txt',
                                '--outdir', output_dir, tmp_file.name
                            ], check=True, timeout=30)
                            
                            txt_file = os.path.join(output_dir, 
                                os.path.basename(tmp_file.name).replace('.ppt', '.txt'))
                            
                            text = ""
                            if os.path.exists(txt_file):
                                with open(txt_file, 'r', encoding='utf-8') as f:
                                    text = f.read()
                            
                            # クリーンアップ
                            os.unlink(tmp_file.name)
                            if os.path.exists(txt_file):
                                os.unlink(txt_file)
                            os.rmdir(output_dir)
                            
                            return text
                    except Exception as e:
                        print(f"Error extracting PPT text from {url}: {e}")
                        return ""
                
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    return await loop.run_in_executor(executor, extract_sync)
            
            async def extract_links(self, url: str) -> List[str]:
                """URLからリンクを抽出"""
                try:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return []
                        
                        content = await response.text()
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        links = []
                        for link in soup.find_all('a', href=True):
                            href = link['href']
                            absolute_url = urljoin(url, href)
                            
                            if self.is_target_domain(absolute_url):
                                # 対象ファイル拡張子をチェック
                                if any(absolute_url.lower().endswith(ext) for ext in 
                                      ['.html', '.pdf', '.docx', '.pptx', '.ppt', '.doc']):
                                    links.append(absolute_url)
                                elif not any(absolute_url.lower().endswith(ext) for ext in
                                           ['.jpg', '.png', '.

### https://www.mhlw.go.jp/ から 各URLをクロールしていきたい

```yaml
name: Advanced Medical Term Crawler and Analysis

on:
  schedule:
    - cron: '0 2 * * *'  # 毎日午前2時に実行
  workflow_dispatch:

jobs:
  crawl-and-analyze:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
        
    - name: Set up JDK 11
      uses: actions/setup-java@v4
      with:
        java-version: '11'
        distribution: 'temurin'
        
    - name: Install Python dependencies
      run: |
        pip install --upgrade pip
        pip install supabase requests beautifulsoup4 python-docx python-pptx PyPDF2 aiohttp asyncio
        
    - name: Install system dependencies
      run: |
        sudo apt-get update
        sudo apt-get install -y wget curl maven libreoffice
        
    - name: Download Sudachi
      run: |
        wget https://github.com/WorksApplications/Sudachi/releases/download/v0.7.3/sudachi-0.7.3-executable.jar
        mkdir -p sudachi
        mv sudachi-0.7.3-executable.jar sudachi/
        
    - name: Download Sudachi dictionaries
      run: |
        cd sudachi
        wget https://github.com/WorksApplications/SudachiDict/releases/download/v20231110/sudachi-dictionary-20231110-full.zip
        unzip sudachi-dictionary-20231110-full.zip
        
    - name: Create Sudachi config
      run: |
        cat > sudachi/sudachi.json << 'EOF'
        {
          "systemDict": "system_full.dic",
          "characterDefinitionFile": "char.def",
          "inputTextPlugin": [
            {
              "class": "com.worksap.nlp.sudachi.DefaultInputTextPlugin"
            }
          ],
          "oovProviderPlugin": [
            {
              "class": "com.worksap.nlp.sudachi.MeCabOovProviderPlugin",
              "charDef": "char.def",
              "unkDef": "unk.def"
            }
          ],
          "pathRewritePlugin": [
            {
              "class": "com.worksap.nlp.sudachi.JoinNumericPlugin"
            },
            {
              "class": "com.worksap.nlp.sudachi.JoinKatakanaOovPlugin"
            }
          ]
        }
        EOF
        
    - name: Create comprehensive crawler
      run: |
        cat > crawler.py << 'EOF'
        import asyncio
        import aiohttp
        import hashlib
        import os
        import tempfile
        from urllib.parse import urljoin, urlparse, parse_qs
        from bs4 import BeautifulSoup
        from concurrent.futures import ThreadPoolExecutor
        from supabase import create_client, Client
        import PyPDF2
        from docx import Document
        from pptx import Presentation
        import subprocess
        from typing import Set, List
        import json
        import re
        from datetime import datetime

        class MHLWCrawler:
            def __init__(self):
                self.supabase: Client = create_client(
                    os.getenv("SUPABASE_URL"),
                    os.getenv("SUPABASE_ANON_KEY")
                )
                self.crawled_urls: Set[str] = self.load_crawled_urls()
                self.session = None
                self.base_url = "https://www.mhlw.go.jp"
                self.processed_count = 0
                self.content_buffer = []
                
            def load_crawled_urls(self) -> Set[str]:
                """既にクロールしたURLをデータベースから取得"""
                try:
                    result = self.supabase.table('crawled_urls').select('url').execute()
                    urls = {row['url'] for row in result.data}
                    print(f"Loaded {len(urls)} previously crawled URLs")
                    return urls
                except Exception as e:
                    print(f"Error loading crawled URLs: {e}")
                    return set()
            
            def save_crawled_url(self, url: str, content_hash: str):
                """クロールしたURLを保存"""
                try:
                    self.supabase.table('crawled_urls').insert({
                        'url': url,
                        'content_hash': content_hash,
                        'crawled_at': datetime.now().isoformat()
                    }).execute()
                    self.crawled_urls.add(url)
                except Exception as e:
                    print(f"Error saving crawled URL {url}: {e}")
            
            async def start_crawling(self):
                """メインクロール開始点"""
                print("Starting MHLW comprehensive crawl...")
                
                connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
                timeout = aiohttp.ClientTimeout(total=60)
                
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                ) as session:
                    self.session = session
                    
                    # 主要なエントリーポイントからクロール開始
                    entry_points = [
                        f"{self.base_url}/",
                        f"{self.base_url}/stf/index.html",
                        f"{self.base_url}/stf/seisakunitsuite/bunya/kenkou_iryou/",
                        f"{self.base_url}/stf/seisakunitsuite/bunya/0000161801.html",
                        f"{self.base_url}/stf/newpage/",
                        f"{self.base_url}/content/",
                        f"{self.base_url}/file/",
                        f"{self.base_url}/topics/",
                        f"{self.base_url}/houdou/",
                        f"{self.base_url}/shingi/"
                    ]
                    
                    # 並列でエントリーポイントをクロール
                    tasks = []
                    for entry_point in entry_points:
                        task = asyncio.create_task(
                            self.crawl_section(entry_point, max_depth=4)
                        )
                        tasks.append(task)
                    
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # バッファに残ったコンテンツを保存
                    if self.content_buffer:
                        await self.flush_content_buffer()
                    
                    print(f"Crawling completed. Processed {self.processed_count} documents.")
            
            async def crawl_section(self, start_url: str, max_depth: int):
                """セクションを再帰的にクロール"""
                visited = set()
                queue = [(start_url, 0)]
                
                while queue:
                    url, depth = queue.pop(0)
                    
                    if depth > max_depth or url in visited or url in self.crawled_urls:
                        continue
                    
                    if not self.is_mhlw_url(url):
                        continue
                    
                    visited.add(url)
                    
                    try:
                        await self.process_document(url)
                        
                        # 新しいリンクを発見してキューに追加
                        if depth < max_depth:
                            new_links = await self.discover_links(url)
                            for link in new_links:
                                if link not in visited and link not in self.crawled_urls:
                                    queue.append((link, depth + 1))
                        
                        await asyncio.sleep(0.3)  # レート制限
                        
                    except Exception as e:
                        print(f"Error processing {url}: {e}")
            
            def is_mhlw_url(self, url: str) -> bool:
                """MHLWのURLかチェック"""
                parsed = urlparse(url)
                return 'mhlw.go.jp' in parsed.netloc
            
            async def discover_links(self, url: str) -> List[str]:
                """URLから新しいリンクを発見"""
                try:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return []
                        
                        content_type = response.headers.get('content-type', '').lower()
                        if 'text/html' not in content_type:
                            return []
                        
                        html_content = await response.text()
                        soup = BeautifulSoup(html_content, 'html.parser')
                        
                        links = set()
                        
                        # <a>タグからリンクを抽出
                        for a_tag in soup.find_all('a', href=True):
                            href = a_tag['href']
                            absolute_url = urljoin(url, href)
                            
                            if self.is_target_document(absolute_url):
                                links.add(absolute_url)
                        
                        # iframe、embed等からもリンクを抽出
                        for tag in soup.find_all(['iframe', 'embed'], src=True):
                            src = tag['src']
                            absolute_url = urljoin(url, src)
                            if self.is_target_document(absolute_url):
                                links.add(absolute_url)
                        
                        return list(links)
                        
                except Exception as e:
                    print(f"Error discovering links from {url}: {e}")
                    return []
            
            def is_target_document(self, url: str) -> bool:
                """対象ドキュメントかチェック"""
                parsed = urlparse(url)
                
                # MHLW以外は除外
                if 'mhlw.go.jp' not in parsed.netloc:
                    return False
                
                # 不要なファイルタイプを除外
                excluded_extensions = {
                    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico',
                    '.css', '.js', '.xml', '.rss', '.zip', '.exe', '.msi'
                }
                
                path_lower = parsed.path.lower()
                if any(path_lower.endswith(ext) for ext in excluded_extensions):
                    return False
                
                # 対象ファイルタイプ
                target_extensions = {
                    '.html', '.htm', '.pdf', '.docx', '.pptx', '.ppt', '.doc', '.xls', '.xlsx'
                }
                
                # 拡張子がない場合はHTMLとみなす
                if any(path_lower.endswith(ext) for ext in target_extensions) or '.' not in path_lower.split('/')[-1]:
                    return True
                
                return False
            
            async def process_document(self, url: str):
                """ドキュメントを処理"""
                try:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return
                        
                        content = await response.read()
                        content_hash = hashlib.md5(content).hexdigest()
                        content_type = response.headers.get('content-type', '').lower()
                        
                        text_content = ""
                        
                        if 'text/html' in content_type:
                            text_content = await self.extract_html_text(content)
                        elif 'application/pdf' in content_type:
                            text_content = await self.extract_pdf_text(content, url)
                        elif 'wordprocessingml.document' in content_type:
                            text_content = await self.extract_docx_text(content, url)
                        elif 'presentationml.presentation' in content_type:
                            text_content = await self.extract_pptx_text(content, url)
                        elif 'application/vnd.ms-powerpoint' in content_type:
                            text_content = await self.extract_ppt_text(content, url)
                        elif 'application/vnd.ms-excel' in content_type:
                            text_content = await self.extract_excel_text(content, url)
                        
                        if text_content and len(text_content.strip()) > 100:
                            self.content_buffer.append({
                                'url': url,
                                'content': text_content[:50000],  # 長すぎるコンテンツは制限
                                'content_type': content_type,
                                'extracted_at': datetime.now().isoformat()
                            })
                            
                            self.save_crawled_url(url, content_hash)
                            self.processed_count += 1
                            
                            if len(self.content_buffer) >= 10:  # バッチ処理
                                await self.flush_content_buffer()
                            
                            print(f"Processed: {url} ({len(text_content)} chars)")
                        
                except Exception as e:
                    print(f"Error processing document {url}: {e}")
            
            async def extract_html_text(self, content: bytes) -> str:
                """HTMLからテキスト抽出"""
                try:
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # 不要な要素を削除
                    for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                        element.decompose()
                    
                    # メインコンテンツを優先的に取得
                    main_content = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile('content|main'))
                    
                    if main_content:
                        text = main_content.get_text(separator=' ', strip=True)
                    else:
                        text = soup.get_text(separator=' ', strip=True)
                    
                    return ' '.join(text.split())
                except Exception as e:
                    print(f"Error extracting HTML text: {e}")
                    return ""
            
            async def extract_pdf_text(self, content: bytes, url: str) -> str:
                """PDFからテキスト抽出"""
                def extract_sync():
                    try:
                        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                            tmp_file.write(content)
                            tmp_file.flush()
                            
                            text = ""
                            with open(tmp_file.name, 'rb') as file:
                                reader = PyPDF2.PdfReader(file)
                                for page_num, page in enumerate(reader.pages):
                                    if page_num > 100:  # ページ数制限
                                        break
                                    try:
                                        text += page.extract_text() + "\n"
                                    except:
                                        continue
                            
                            os.unlink(tmp_file.name)
                            return text
                    except Exception as e:
                        print(f"Error extracting PDF text from {url}: {e}")
                        return ""
                
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor(max_workers=2) as executor:
                    return await loop.run_in_executor(

### gh workflow run すると、どんな結果になりますか？


```bash
# GitHub Actions実行結果の予想される出力

Run crawler.py
Starting MHLW comprehensive crawl...
Loaded 0 previously crawled URLs
Processing entry point: https://www.mhlw.go.jp/
Processing entry point: https://www.mhlw.go.jp/stf/index.html
Processing entry point: https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/
Processing entry point: https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000161801.html
Processing entry point: https://www.mhlw.go.jp/stf/newpage/
Processing entry point: https://www.mhlw.go.jp/content/
Processing entry point: https://www.mhlw.go.jp/file/
Processing entry point: https://www.mhlw.go.jp/topics/
Processing entry point: https://www.mhlw.go.jp/houdou/
Processing entry point: https://www.mhlw.go.jp/shingi/

Processed: https://www.mhlw.go.jp/ (15432 chars)
Processed: https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/ (23567 chars)
Processed: https://www.mhlw.go.jp/content/000123456.pdf (45678 chars)
Error processing https://www.mhlw.go.jp/file/invalid.pdf: HTTP 404
Processed: https://www.mhlw.go.jp/stf/newpage_12345.html (12890 chars)
Processed: https://www.mhlw.go.jp/content/000789123.docx (8765 chars)

Discovering links from https://www.mhlw.go.jp/
Found 127 new links to process
Discovering links from https://www.mhlw.go.jp/stf/index.html
Found 89 new links to process

Processed: https://www.mhlw.go.jp/topics/2024/01/tp0115-1.html (9876 chars)
Processed: https://www.mhlw.go.jp/houdou/2024/01/h0115-2.html (15432 chars)
Error extracting PDF text from https://www.mhlw.go.jp/content/broken.pdf: Invalid PDF format
Processed: https://www.mhlw.go.jp/shingi/2024/01/s0115-3.html (21098 chars)

Saved content batch to database (10 documents)
Saved content batch to database (10 documents)
Saved content batch to database (8 documents)

Crawling completed. Processed 245 documents.

Run morphological_analyzer.py
Starting morphological analysis...
Loading content from database...
Retrieved 245 documents for analysis

Processing document batch 1/25...
Analyzing: https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/
Found 45 potential medical terms
Processing document batch 2/25...
Analyzing: https://www.mhlw.go.jp/content/000123456.pdf
Found 67 potential medical terms

Sudachi analysis results:
- Total sentences processed: 12,567
- Total morphemes extracted: 145,890
- Medical term candidates: 2,341
- Filtered medical terms: 1,234

Medical term examples found:
- 新型コロナウイルス感染症
- ワクチン接種
- 副反応疑い報告
- mRNA疫苗
- 中和抗体価
- オミクロン株
- デルタ変異株
- 重篤副反応
- アナフィラキシー
- 心筋炎・心膜炎

Morphological analysis completed.

Run term_validator.py
Starting new term validation...
Connecting to Supabase database...
Loading existing medical terms...
Found 15,432 existing terms in database

Comparing with discovered terms...
New terms found: 89
Previously unknown terms: 89

Validating new terms:
✓ ブレークスルー感染 (breakthrough infection)
✓ ロングCOVID (long COVID)
✓ BA.5系統 (BA.5 lineage)
✓ 3回目追加接種 (third booster shot)
✓ 小児用製剤 (pediatric formulation)
✓ 交差免疫 (cross immunity)
✓ 変異株監視 (variant surveillance)
✗ 一般的用語 (filtered out)
✗ 既存用語 (already exists)

Final new terms to save: 67

Saving new terms to database...
Successfully saved 67 new medical terms
Updated term discovery log

Validation completed.

Run database_handler.py
Updating database with analysis results...
Connected to Supabase successfully

Statistics update:
- Documents processed: 245
- New URLs crawled: 245  
- New terms discovered: 67
- Total terms in database: 15,499
- Last update: 2024-01-15T02:30:45Z

Database operations completed successfully.

Upload artifacts
Uploading analysis results...
✓ crawled_urls.json (245 URLs)
✓ extracted_content.json (245 documents)  
✓ new_terms.json (67 terms)
✓ analysis_log.txt (processing details)
✓ error_log.txt (12 errors recorded)

Workflow completed successfully in 18m 32s

# 期待される成果物
# 1. Supabaseデータベースに保存されるデータ:
#    - crawled_urls テーブル: 245件の新しいURL
#    - medical_terms テーブル: 67件の新語
#    - document_content テーブル: 245件のコンテンツ
#    - crawl_statistics テーブル: 実行統計

# 2. GitHub Artifactsにアップロードされるファイル:
#    - 抽出されたコンテンツのJSON
#    - 発見された新語のリスト
#    - クロール統計情報
#    - エラーログ

# 3. 実際に発見される可能性のある医療新語例:
#    - COVID-19関連の最新用語
#    - 新薬・治療法の専門用語
#    - 厚労省の政策用語
#    - 医療制度の新しい概念語
```

実際の実行では以下のような課題が発生する可能性があります：

1. **レート制限**: 厚労省サイトからのアクセス制限
2. **メモリ制限**: GitHub Actionsの2GB制限
3. **実行時間制限**: 6時間のタイムアウト
4. **PDF/Office文書の解析エラー**: 破損ファイルや特殊フォーマット
5. **Sudachi辞書の日本語処理**: 新語の適切な分割

成功した場合、定期的に厚労省の最新医療用語を自動収集・分析できるシステムが構築されます。

### morphological_analyzer.py database_handler.py の codeを提案して下さい。

```python
# morphological_analyzer.py
import asyncio
import subprocess
import tempfile
import json
import re
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from supabase import create_client, Client
import os
from datetime import datetime

class MorphologicalAnalyzer:
    def __init__(self):
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_ANON_KEY")
        )
        self.sudachi_jar = "sudachi/sudachi-0.7.3-executable.jar"
        self.sudachi_config = "sudachi/sudachi.json"
        self.medical_keywords = self.load_medical_keywords()
        
    def load_medical_keywords(self) -> Set[str]:
        """医療関連キーワードの読み込み"""
        keywords = {
            # 医療基本用語
            "病", "症", "疾", "患", "診", "療", "治", "薬", "剤", "接種",
            "検査", "手術", "処置", "副作用", "副反応", "感染", "予防",
            "ワクチン", "抗体", "免疫", "ウイルス", "細菌", "がん", "癌",
            
            # COVID-19関連
            "コロナ", "COVID", "SARS", "MERS", "オミクロン", "デルタ",
            "変異株", "系統", "株", "感染者", "陽性", "陰性", "PCR",
            "抗原", "中和", "mRNA", "ワクチン", "ブースター", "追加接種",
            
            # 医療制度・政策
            "保険", "医療費", "診療報酬", "薬価", "承認", "認可", "適応",
            "禁忌", "注意", "警告", "指針", "ガイドライン", "基準",
            
            # 専門医療用語接尾辞
            "炎", "腫", "瘍", "痛", "熱", "血", "尿", "便", "液", "圧"
        }
        
        return keywords
    
    async def analyze_all_content(self):
        """全コンテンツの形態素解析を実行"""
        print("Starting morphological analysis...")
        
        # データベースからコンテンツを取得
        content_data = await self.load_content_from_database()
        print(f"Retrieved {len(content_data)} documents for analysis")
        
        if not content_data:
            print("No content found for analysis")
            return
        
        # 並列処理で形態素解析を実行
        batch_size = 10
        all_terms = set()
        processed_count = 0
        
        for i in range(0, len(content_data), batch_size):
            batch = content_data[i:i + batch_size]
            print(f"Processing document batch {i//batch_size + 1}/{(len(content_data)-1)//batch_size + 1}...")
            
            batch_terms = await self.process_batch(batch)
            all_terms.update(batch_terms)
            processed_count += len(batch)
            
            print(f"Batch completed. Found {len(batch_terms)} terms. Total processed: {processed_count}")
        
        print(f"Morphological analysis completed. Total unique terms: {len(all_terms)}")
        
        # 結果を保存
        await self.save_analysis_results(all_terms)
        
        return list(all_terms)
    
    async def load_content_from_database(self) -> List[Dict]:
        """データベースからコンテンツを取得"""
        try:
            result = self.supabase.table('document_content')\
                .select('url, content, content_type')\
                .order('extracted_at', desc=True)\
                .limit(1000)\
                .execute()
            
            return result.data
        except Exception as e:
            print(f"Error loading content from database: {e}")
            return []
    
    async def process_batch(self, batch: List[Dict]) -> Set[str]:
        """バッチ単位でドキュメントを処理"""
        loop = asyncio.get_event_loop()
        
        with ProcessPoolExecutor(max_workers=4) as executor:
            tasks = []
            for doc in batch:
                task = loop.run_in_executor(
                    executor, 
                    self.analyze_document_sync, 
                    doc['content'], 
                    doc['url']
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 結果をマージ
        all_terms = set()
        for result in results:
            if isinstance(result, set):
                all_terms.update(result)
            elif isinstance(result, Exception):
                print(f"Error in batch processing: {result}")
        
        return all_terms
    
    def analyze_document_sync(self, content: str, url: str) -> Set[str]:
        """同期的にドキュメントを解析"""
        try:
            # テキストを文単位に分割
            sentences = self.split_into_sentences(content)
            
            medical_terms = set()
            
            # 各文を形態素解析
            for sentence in sentences:
                if len(sentence.strip()) < 5:
                    continue
                
                morphemes = self.analyze_with_sudachi(sentence)
                
                # 医療用語を抽出
                terms = self.extract_medical_terms(morphemes)
                medical_terms.update(terms)
            
            print(f"Analyzed: {url} - Found {len(medical_terms)} terms")
            return medical_terms
            
        except Exception as e:
            print(f"Error analyzing document {url}: {e}")
            return set()
    
    def split_into_sentences(self, text: str) -> List[str]:
        """テキストを文に分割"""
        # 日本語の文区切り文字で分割
        sentences = re.split(r'[。！？\n]', text)
        
        # 空文字や短すぎる文を除外
        return [s.strip() for s in sentences if len(s.strip()) > 10]
    
    def analyze_with_sudachi(self, text: str) -> List[Dict]:
        """Sudachiで形態素解析を実行"""
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp_file:
                tmp_file.write(text)
                tmp_file.flush()
                
                # Sudachiコマンド実行
                cmd = [
                    'java', '-jar', self.sudachi_jar,
                    '-r', self.sudachi_config,
                    '-m', 'A',  # 短単位モード
                    '-a', tmp_file.name
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=30)
                
                # 一時ファイル削除
                os.unlink(tmp_file.name)
                
                if result.returncode != 0:
                    print(f"Sudachi error: {result.stderr}")
                    return []
                
                return self.parse_sudachi_output(result.stdout)
                
        except Exception as e:
            print(f"Error in Sudachi analysis: {e}")
            return []
    
    def parse_sudachi_output(self, output: str) -> List[Dict]:
        """Sudachiの出力を解析"""
        morphemes = []
        
        for line in output.strip().split('\n'):
            if not line or line.startswith('#'):
                continue
            
            parts = line.split('\t')
            if len(parts) >= 6:
                morpheme = {
                    'surface': parts[0],          # 表層形
                    'part_of_speech': parts[1],   # 品詞
                    'reading': parts[2] if len(parts) > 2 else '',
                    'base_form': parts[3] if len(parts) > 3 else parts[0],
                    'features': parts[4:] if len(parts) > 4 else []
                }
                morphemes.append(morpheme)
        
        return morphemes
    
    def extract_medical_terms(self, morphemes: List[Dict]) -> Set[str]:
        """形態素から医療用語を抽出"""
        medical_terms = set()
        
        for i, morpheme in enumerate(morphemes):
            surface = morpheme['surface']
            pos = morpheme['part_of_speech']
            base_form = morpheme['base_form']
            
            # 基本的な医療用語判定
            if self.is_medical_term(surface, pos):
                medical_terms.add(surface)
                if base_form != surface:
                    medical_terms.add(base_form)
            
            # 複合語の検出
            compound_term = self.check_compound_medical_term(morphemes, i)
            if compound_term:
                medical_terms.add(compound_term)
        
        # フィルタリング
        return self.filter_medical_terms(medical_terms)
    
    def is_medical_term(self, term: str, pos: str) -> bool:
        """医療用語かどうかの判定"""
        # 長さチェック
        if len(term) < 2 or len(term) > 30:
            return False
        
        # 数字のみ、記号のみは除外
        if term.isdigit() or not re.search(r'[あ-んア-ンヴー一-龯]', term):
            return False
        
        # 品詞チェック（名詞、形容詞、動詞の一部）
        if not any(pos.startswith(p) for p in ['名詞', '形容詞', '動詞']):
            return False
        
        # 医療キーワードを含むかチェック
        if any(keyword in term for keyword in self.medical_keywords):
            return True
        
        # カタカナ医学用語（4文字以上）
        if re.match(r'^[ア-ンヴー]{4,}$', term):
            return True
        
        # 英数字混在の医学用語（COVID-19, mRNAなど）
        if re.search(r'[A-Za-z]', term) and re.search(r'[0-9ア-ンヴー一-龯]', term):
            return True
        
        return False
    
    def check_compound_medical_term(self, morphemes: List[Dict], start_idx: int) -> str:
        """複合医療用語の検出"""
        if start_idx >= len(morphemes) - 1:
            return ""
        
        # 最大5語までの複合語をチェック
        for length in range(2, min(6, len(morphemes) - start_idx + 1)):
            compound_parts = []
            is_medical_compound = True
            
            for i in range(length):
                morpheme = morphemes[start_idx + i]
                surface = morpheme['surface']
                pos = morpheme['part_of_speech']
                
                compound_parts.append(surface)
                
                # 複合語の各部分が医療関連または連結可能な語かチェック
                if not (self.is_medical_term(surface, pos) or 
                       pos.startswith('名詞') or 
                       surface in ['の', 'による', 'に関する']):
                    is_medical_compound = False
                    break
            
            if is_medical_compound:
                compound_term = ''.join(compound_parts)
                
                # 複合語としての医療用語判定
                if (len(compound_term) >= 4 and 
                    any(keyword in compound_term for keyword in self.medical_keywords)):
                    return compound_term
        
        return ""
    
    def filter_medical_terms(self, terms: Set[str]) -> Set[str]:
        """医療用語のフィルタリング"""
        filtered_terms = set()
        
        for term in terms:
            # 一般的すぎる語を除外
            common_words = {
                'こと', 'もの', 'とき', 'ところ', 'ため', 'など', 'について',
                '場合', '状況', '問題', '結果', '方法', '必要', '重要', '可能'
            }
            
            if term in common_words:
                continue
            
            # HTMLタグやURLの残骸を除外
            if any(char in term for char in ['<', '>', 'http', 'www', '.com', '.jp']):
                continue
            
            # 医療用語らしさのスコアリング
            score = self.calculate_medical_term_score(term)
            if score >= 0.3:  # 閾値
                filtered_terms.add(term)
        
        return filtered_terms
    
    def calculate_medical_term_score(self, term: str) -> float:
        """医療用語らしさのスコア計算"""
        score = 0.0
        
        # 医療キーワード含有度
        keyword_matches = sum(1 for keyword in self.medical_keywords if keyword in term)
        score += keyword_matches * 0.3
        
        # カタカナ比率（医学専門用語の特徴）
        katakana_ratio = len(re.findall(r'[ア-ンヴー]', term)) / len(term)
        if katakana_ratio > 0.5:
            score += 0.2
        
        # 漢字比率（日本の医学用語の特徴）
        kanji_ratio = len(re.findall(r'[一-龯]', term)) / len(term)
        if 0.3 <= kanji_ratio <= 0.8:
            score += 0.2
        
        # 長さボーナス（専門用語は一定の長さがある）
        if 3 <= len(term) <= 10:
            score += 0.1
        elif 10 < len(term) <= 20:
            score += 0.05
        
        # 英数字混在（COVID-19, mRNAなど）
        if re.search(r'[A-Za-z]', term) and re.search(r'[0-9]', term):
            score += 0.3
        
        return score
    
    async def save_analysis_results(self, terms: Set[str]):
        """解析結果を保存"""
        try:
            # 結果をファイルに保存
            terms_list = sorted(list(terms))
            
            with open('analysis_results.json', 'w', encoding='utf-8') as f:
                json.dump({
                    'total_terms': len(terms_list),
                    'analysis_date': datetime.now().isoformat(),
                    'terms': terms_list
                }, f, ensure_ascii=False, indent=2)
            
            print(f"Saved {len(terms_list)} terms to analysis_results.json")
            
            # 

### webクローラーも並行処理できないか？

```python
# advanced_crawler.py
import asyncio
import aiohttp
import aiofiles
import hashlib
import os
import tempfile
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from supabase import create_client, Client
import PyPDF2
from docx import Document
from pptx import Presentation
import subprocess
from typing import Set, List, Dict, Tuple, Optional
import json
import re
from datetime import datetime
import time
from dataclasses import dataclass
from queue import PriorityQueue
import threading
from contextlib import asynccontextmanager

@dataclass
class CrawlJob:
    url: str
    depth: int
    priority: int
    parent_url: str = ""
    
    def __lt__(self, other):
        return self.priority < other.priority

class AdvancedMHLWCrawler:
    def __init__(self, max_workers: int = 20, max_concurrent_downloads: int = 10):
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_ANON_KEY")
        )
        
        # 並行処理設定
        self.max_workers = max_workers
        self.max_concurrent_downloads = max_concurrent_downloads
        self.semaphore = None
        self.download_semaphore = None
        
        # クロール状態管理
        self.crawled_urls: Set[str] = set()
        self.processing_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        self.url_queue = asyncio.Queue(maxsize=1000)
        
        # 統計情報
        self.stats = {
            'processed': 0,
            'errors': 0,
            'documents_saved': 0,
            'start_time': None,
            'links_discovered': 0
        }
        
        # レート制限
        self.last_request_time = {}
        self.min_delay = 0.2  # 最小遅延時間（秒）
        
        self.base_url = "https://www.mhlw.go.jp"
        self.session_pool = []
        
    async def initialize(self):
        """初期化処理"""
        print("Initializing advanced crawler...")
        
        # セマフォ初期化
        self.semaphore = asyncio.Semaphore(self.max_workers)
        self.download_semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        # 既存URLロード
        await self.load_crawled_urls()
        
        # セッションプール作成
        await self.create_session_pool()
        
        self.stats['start_time'] = time.time()
        print(f"Crawler initialized with {self.max_workers} workers")
    
    async def load_crawled_urls(self):
        """既にクロールしたURLを非同期で読み込み"""
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self.supabase.table('crawled_urls').select('url').execute()
            )
            self.crawled_urls = {row['url'] for row in result.data}
            print(f"Loaded {len(self.crawled_urls)} previously crawled URLs")
        except Exception as e:
            print(f"Error loading crawled URLs: {e}")
            self.crawled_urls = set()
    
    async def create_session_pool(self):
        """HTTPセッションプールを作成"""
        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=10,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(
            total=60,
            connect=10,
            sock_read=30
        )
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        # 複数のセッションを作成してプールする
        for _ in range(5):
            session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )
            self.session_pool.append(session)
    
    @asynccontextmanager
    async def get_session(self):
        """セッションプールからセッションを取得"""
        if not self.session_pool:
            await self.create_session_pool()
        
        session = self.session_pool.pop(0)
        try:
            yield session
        finally:
            self.session_pool.append(session)
    
    async def start_comprehensive_crawl(self):
        """包括的なクロールを開始"""
        await self.initialize()
        
        print("Starting comprehensive MHLW crawl with advanced parallel processing...")
        
        # エントリーポイントをキューに追加
        entry_points = [
            (f"{self.base_url}/", 0, 1),
            (f"{self.base_url}/stf/", 0, 1),
            (f"{self.base_url}/stf/seisakunitsuite/bunya/kenkou_iryou/", 0, 2),
            (f"{self.base_url}/stf/seisakunitsuite/bunya/0000161801.html", 0, 2),
            (f"{self.base_url}/content/", 0, 1),
            (f"{self.base_url}/file/", 0, 1),
            (f"{self.base_url}/topics/", 0, 1),
            (f"{self.base_url}/houdou/", 0, 1),
            (f"{self.base_url}/shingi/", 0, 1),
            (f"{self.base_url}/wp/", 0, 1),
            (f"{self.base_url}/bunya/", 0, 1)
        ]
        
        for url, depth, priority in entry_points:
            await self.url_queue.put(CrawlJob(url, depth, priority))
        
        # ワーカータスクを起動
        workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(self.worker(f"Worker-{i}"))
            workers.append(worker)
        
        # 統計レポートタスク
        stats_task = asyncio.create_task(self.stats_reporter())
        
        # URL発見タスク
        discovery_task = asyncio.create_task(self.url_discovery_worker())
        
        try:
            # すべてのワーカーが完了するまで待機
            await asyncio.gather(*workers, return_exceptions=True)
            
        finally:
            # クリーンアップ
            stats_task.cancel()
            discovery_task.cancel()
            await self.cleanup()
        
        await self.print_final_stats()
    
    async def worker(self, worker_name: str):
        """ワーカータスク"""
        print(f"{worker_name} started")
        
        consecutive_empty = 0
        max_consecutive_empty = 10
        
        while consecutive_empty < max_consecutive_empty:
            try:
                # キューからジョブを取得（タイムアウト付き）
                job = await asyncio.wait_for(self.url_queue.get(), timeout=5.0)
                consecutive_empty = 0
                
                async with self.semaphore:
                    await self.process_crawl_job(job, worker_name)
                
                self.url_queue.task_done()
                
            except asyncio.TimeoutError:
                consecutive_empty += 1
                if consecutive_empty % 5 == 0:
                    print(f"{worker_name} waiting... (queue empty count: {consecutive_empty})")
                continue
            
            except Exception as e:
                print(f"{worker_name} error: {e}")
                self.stats['errors'] += 1
                
        print(f"{worker_name} finished")
    
    async def process_crawl_job(self, job: CrawlJob, worker_name: str):
        """クロールジョブを処理"""
        url = job.url
        
        # 重複チェック
        if url in self.crawled_urls or url in self.processing_urls:
            return
        
        if not self.is_valid_mhlw_url(url):
            return
        
        self.processing_urls.add(url)
        
        try:
            # レート制限
            await self.rate_limit(url)
            
            # ドキュメント処理
            success = await self.process_document_parallel(url, worker_name)
            
            if success:
                self.crawled_urls.add(url)
                self.stats['processed'] += 1
                
                # 新しいリンクを発見してキューに追加（深度制限あり）
                if job.depth < 4:
                    await self.discover_and_queue_links(url, job.depth + 1)
            else:
                self.failed_urls.add(url)
                self.stats['errors'] += 1
                
        except Exception as e:
            print(f"Error processing {url} in {worker_name}: {e}")
            self.failed_urls.add(url)
            self.stats['errors'] += 1
            
        finally:
            self.processing_urls.discard(url)
    
    async def rate_limit(self, url: str):
        """ドメイン別レート制限"""
        domain = urlparse(url).netloc
        current_time = time.time()
        
        if domain in self.last_request_time:
            elapsed = current_time - self.last_request_time[domain]
            if elapsed < self.min_delay:
                await asyncio.sleep(self.min_delay - elapsed)
        
        self.last_request_time[domain] = time.time()
    
    def is_valid_mhlw_url(self, url: str) -> bool:
        """有効なMHLWのURLかチェック"""
        parsed = urlparse(url)
        
        if 'mhlw.go.jp' not in parsed.netloc:
            return False
        
        # 除外パターン
        excluded_patterns = [
            '/css/', '/js/', '/images/', '/img/', '/static/',
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico',
            '.css', '.js', '.xml', '.rss', '.zip', '.exe',
            'javascript:', 'mailto:', 'tel:', 'fax:'
        ]
        
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in excluded_patterns):
            return False
        
        return True
    
    async def process_document_parallel(self, url: str, worker_name: str) -> bool:
        """並列ドキュメント処理"""
        try:
            async with self.download_semaphore:
                async with self.get_session() as session:
                    async with session.get(url) as response:
                        if response.status != 200:
                            return False
                        
                        content_type = response.headers.get('content-type', '').lower()
                        content_length = response.headers.get('content-length', '0')
                        
                        # ファイルサイズ制限（50MB）
                        if int(content_length) > 50 * 1024 * 1024:
                            print(f"Skipping large file: {url} ({content_length} bytes)")
                            return False
                        
                        content = await response.read()
                        
            # 非同期でテキスト抽出
            text_content = await self.extract_text_async(content, content_type, url)
            
            if text_content and len(text_content.strip()) > 100:
                # バックグラウンドでデータベース保存
                asyncio.create_task(
                    self.save_document_async(url, text_content, content_type)
                )
                
                print(f"{worker_name}: Processed {url} ({len(text_content)} chars)")
                return True
            
            return False
            
        except Exception as e:
            print(f"Error in parallel document processing {url}: {e}")
            return False
    
    async def extract_text_async(self, content: bytes, content_type: str, url: str) -> str:
        """非同期テキスト抽出"""
        loop = asyncio.get_event_loop()
        
        # CPU集約的なタスクを別プロセスで実行
        with ProcessPoolExecutor(max_workers=4) as executor:
            if 'text/html' in content_type:
                return await loop.run_in_executor(
                    executor, self.extract_html_text_sync, content
                )
            elif 'application/pdf' in content_type:
                return await loop.run_in_executor(
                    executor, self.extract_pdf_text_sync, content, url
                )
            elif 'wordprocessingml.document' in content_type:
                return await loop.run_in_executor(
                    executor, self.extract_docx_text_sync, content, url
                )
            elif 'presentationml.presentation' in content_type:
                return await loop.run_in_executor(
                    executor, self.extract_pptx_text_sync, content, url
                )
            elif 'application/vnd.ms-excel' in content_type:
                return await loop.run_in_executor(
                    executor, self.extract_excel_text_sync, content, url
                )
        
        return ""
    
    def extract_html_text_sync(self, content: bytes) -> str:
        """同期HTML テキスト抽出"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # 不要な要素を削除
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'form']):
                element.decompose()
            
            # メインコンテンツを優先取得
            main_selectors = [
                'main', 'article', '[role="main"]',
                '.main-content', '.content', '.body-content',
                '#main', '#content', '#body'
            ]
            
            main_content = None
            for selector in main_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if main_content:
                text = main_content.get_text(separator=' ', strip=True)
            else:
                text = soup.get_text(separator=' ', strip=True)
            
            # テキストクリーニング
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
            
            return text.strip()
            
        except Exception as e:
            print(f"Error extracting HTML text: {e}")
            return ""
    

### https://claude.ai/share/3df0ed8b-975e-4797-8cf9-d782b1736e68 を読んで参考にして、コードを更に洗練して。

```python
# advanced_crawler.py
import asyncio
import aiohttp
import aiofiles
import hashlib
import os
import tempfile
from urllib.parse import urljoin, urlparse, quote
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from supabase import create_client, Client
import PyPDF2
from docx import Document
from pptx import Presentation
import subprocess
from typing import Set, List, Dict, Tuple, Optional, AsyncGenerator
import json
import re
from datetime import datetime, timedelta
import time
from dataclasses import dataclass, field
import logging
from contextlib import asynccontextmanager
import signal
import sys
from pathlib import Path
import mimetypes

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlJob:
    url: str
    depth: int
    priority: int
    parent_url: str = ""
    retry_count: int = 0
    content_type: Optional[str] = None
    
    def __lt__(self, other):
        return (self.priority, self.retry_count) < (other.priority, other.retry_count)

@dataclass
class CrawlStats:
    start_time: float = field(default_factory=time.time)
    processed: int = 0
    errors: int = 0
    documents_saved: int = 0
    links_discovered: int = 0
    retries: int = 0
    skipped: int = 0
    
    @property
    def runtime(self) -> float:
        return time.time() - self.start_time
    
    @property
    def rate(self) -> float:
        return self.processed / self.runtime if self.runtime > 0 else 0

class RobustHTTPSession:
    """堅牢なHTTPセッション管理"""
    
    def __init__(self, max_connections: int = 50):
        self.connector = aiohttp.TCPConnector(
            limit=max_connections,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
            force_close=True  # メモリリーク防止
        )
        
        self.timeout = aiohttp.ClientTimeout(
            total=90,
            connect=15,
            sock_read=60
        )
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MedicalTermBot/2.0; +https://github.com/medical-research)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        }
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
    
    async def __aenter__(self):
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
    
    async def _ensure_session(self):
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    connector=self.connector,
                    timeout=self.timeout,
                    headers=self.headers,
                    cookie_jar=aiohttp.CookieJar(unsafe=True)
                )
    
    async def get(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        await self._ensure_session()
        return await self._session.get(url, **kwargs)

class ContentExtractor:
    """コンテンツ抽出の専門クラス"""
    
    @staticmethod
    async def extract_text_async(content: bytes, content_type: str, url: str) -> str:
        """非同期でテキストを抽出"""
        loop = asyncio.get_event_loop()
        
        try:
            if 'text/html' in content_type:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_html_text, content
                    )
            elif 'application/pdf' in content_type:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_pdf_text, content
                    )
            elif 'wordprocessingml.document' in content_type or content_type.endswith('.docx'):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_docx_text, content
                    )
            elif 'presentationml.presentation' in content_type or content_type.endswith('.pptx'):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_pptx_text, content
                    )
            
        except Exception as e:
            logger.error(f"Content extraction error for {url}: {e}")
        
        return ""
    
    @staticmethod
    def _extract_html_text(content: bytes) -> str:
        try:
            # エンコーディング検出
            encoding = 'utf-8'
            if b'charset=' in content[:1024]:
                charset_match = re.search(rb'charset=(["\']?)([^"\'>]+)', content[:1024])
                if charset_match:
                    encoding = charset_match.group(2).decode('ascii', errors='ignore')
            
            html_text = content.decode(encoding, errors='ignore')
            soup = BeautifulSoup(html_text, 'lxml')
            
            # 不要要素の削除（より包括的）
            unwanted_tags = [
                'script', 'style', 'nav', 'header', 'footer', 'aside', 
                'form', 'iframe', 'noscript', 'svg', 'canvas'
            ]
            for tag in unwanted_tags:
                for element in soup.find_all(tag):
                    element.decompose()
            
            # メインコンテンツの特定
            main_content = ContentExtractor._find_main_content(soup)
            
            if main_content:
                text = main_content.get_text(separator='\n', strip=True)
            else:
                text = soup.get_text(separator='\n', strip=True)
            
            # テキスト正規化
            text = ContentExtractor._normalize_text(text)
            
            return text
            
        except Exception as e:
            logger.error(f"HTML extraction error: {e}")
            return ""
    
    @staticmethod
    def _find_main_content(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """メインコンテンツを特定"""
        # 優先度順でセレクタを試行
        selectors = [
            'main',
            'article',
            '[role="main"]',
            '.main-content',
            '.content-main',
            '#main-content',
            '#content',
            '.post-content',
            '.entry-content',
            '.page-content'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element and len(element.get_text(strip=True)) > 200:
                return element
        
        # フォールバック: 最も長いテキストを持つdivを選択
        divs = soup.find_all('div')
        if divs:
            longest_div = max(divs, key=lambda d: len(d.get_text(strip=True)))
            if len(longest_div.get_text(strip=True)) > 500:
                return longest_div
        
        return None
    
    @staticmethod
    def _normalize_text(text: str) -> str:
        """テキストの正規化"""
        # 連続する空白文字を単一スペースに
        text = re.sub(r'\s+', ' ', text)
        
        # 制御文字を削除
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
        
        # 行の正規化
        lines = []
        for line in text.split('\n'):
            line = line.strip()
            if line and len(line) > 3:  # 短すぎる行をスキップ
                lines.append(line)
        
        return '\n'.join(lines)
    
    @staticmethod
    def _extract_pdf_text(content: bytes) -> str:
        """PDFテキスト抽出（改良版）"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                text_parts = []
                
                with open(tmp_file.name, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    
                    # ページ数制限
                    max_pages = min(len(reader.pages), 200)
                    
                    for page_num in range(max_pages):
                        try:
                            page = reader.pages[page_num]
                            page_text = page.extract_text()
                            
                            if page_text and len(page_text.strip()) > 50:
                                text_parts.append(page_text)
                                
                        except Exception as e:
                            logger.debug(f"PDF page {page_num} extraction error: {e}")
                            continue
                
                os.unlink(tmp_file.name)
                
                full_text = '\n'.join(text_parts)
                return ContentExtractor._normalize_text(full_text)
                
        except Exception as e:
            logger.error(f"PDF extraction error: {e}")
            return ""
    
    @staticmethod
    def _extract_docx_text(content: bytes) -> str:
        """DOCXテキスト抽出"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                doc = Document(tmp_file.name)
                paragraphs = []
                
                for paragraph in doc.paragraphs:
                    text = paragraph.text.strip()
                    if text and len(text) > 5:
                        paragraphs.append(text)
                
                # テーブルからもテキストを抽出
                for table in doc.tables:
                    for row in table.rows:
                        row_text = []
                        for cell in row.cells:
                            cell_text = cell.text.strip()
                            if cell_text:
                                row_text.append(cell_text)
                        if row_text:
                            paragraphs.append(' | '.join(row_text))
                
                os.unlink(tmp_file.name)
                
                full_text = '\n'.join(paragraphs)
                return ContentExtractor._normalize_text(full_text)
                
        except Exception as e:
            logger.error(f"DOCX extraction error: {e}")
            return ""
    
    @staticmethod
    def _extract_pptx_text(content: bytes) -> str:
        """PPTXテキスト抽出"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.pptx', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                prs = Presentation(tmp_file.name)
                text_parts = []
                
                for slide_num, slide in enumerate(prs.slides):
                    slide_text = []
                    
                    for shape in slide.shapes:
                        if hasattr(shape, "text") and shape.text:
                            slide_text.append(shape.text.strip())
                    
                    if slide_text:
                        text_parts.append(f"--- Slide {slide_num + 1} ---")
                        text_parts.extend(slide_text)
                
                os.unlink(tmp_file.name)
                
                full_text = '\n'.join(text_parts)
                return ContentExtractor._normalize_text(full_text)
                
        except Exception as e:
            logger.error(f"PPTX extraction error: {e}")
            return ""

class AdvancedMHLWCrawler:
    """高度な厚労省クローラー"""
    
    def __init__(self, 
                 max_workers: int = 15,
                 max_concurrent_downloads: int = 8,
                 max_depth: int = 4,
                 delay_range: Tuple[float, float] = (0.5, 1.5)):
        
        # Supabase接続
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_ANON_KEY")
        )
        
        # 並行処理設定
        self.max_workers = max_workers
        self.max_concurrent_downloads = max_concurrent_downloads
        self.max_depth = max_depth
        self.delay_range = delay_range
        
        # 非同期制御
        self.semaphore = asyncio.Semaphore(max_workers)
        self.download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
        
        # 状態管理
        self.crawled_urls: Set[str] = set()
        self.processing_urls: Set[str] = set()
        self.failed_urls: Dict[str, int] = {}  # URL -> retry count
        self.url_queue = asyncio.PriorityQueue(maxsize=2000)
        
        # 統計
        self.stats = CrawlStats()
        
        # robots.txt チェック
        self.robots_parser = None
        
        # レート制限
        self.domain_delays = {}
        
        # シャットダウン制御
        self.shutdown_event = asyncio.Event()
        
        self.base_url = "https://www.mhlw.go.jp"
        
        # シグナルハンドラ設定
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """グレースフルシャットダウン"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self._shutdown())
    

### supabaseへの接続情報は gh secret で管理する方針で。workflow形式を洗練して書き直し。

```yaml
name: Advanced Medical Term Crawler

on:
  schedule:
    - cron: '0 2 * * *'  # 毎日午前2時実行
  workflow_dispatch:
    inputs:
      max_workers:
        description: 'Maximum number of worker threads'
        required: false
        default: '15'
      max_depth:
        description: 'Maximum crawl depth'
        required: false
        default: '4'
      target_domains:
        description: 'Target domains (comma separated)'
        required: false
        default: 'mhlw.go.jp'

env:
  PYTHON_VERSION: '3.11'
  JAVA_VERSION: '11'
  MAX_RUNTIME_MINUTES: 300

jobs:
  setup:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.setup-matrix.outputs.matrix }}
    steps:
      - name: Setup job matrix
        id: setup-matrix
        run: |
          echo "matrix={\"include\":[{\"job\":\"crawler\",\"max_workers\":${{ github.event.inputs.max_workers || '15' }}},{\"job\":\"analyzer\",\"batch_size\":\"50\"}]}" >> $GITHUB_OUTPUT

  dependencies:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 1

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: 'pip'

      - name: Setup Java
        uses: actions/setup-java@v4
        with:
          java-version: ${{ env.JAVA_VERSION }}
          distribution: 'temurin'
          cache: 'maven'

      - name: Cache system dependencies
        uses: actions/cache@v3
        id: cache-system-deps
        with:
          path: |
            /usr/share/java/
            ~/.cache/
          key: system-deps-${{ runner.os }}-${{ hashFiles('**/requirements.txt') }}

      - name: Install system dependencies
        if: steps.cache-system-deps.outputs.cache-hit != 'true'
        run: |
          sudo apt-get update -qq
          sudo apt-get install -y --no-install-recommends \
            libreoffice-headless \
            poppler-utils \
            tesseract-ocr \
            tesseract-ocr-jpn \
            fonts-noto-cjk \
            curl \
            wget \
            unzip

      - name: Install Python dependencies
        run: |
          python -m pip install --upgrade pip setuptools wheel
          pip install --no-cache-dir \
            supabase==1.0.4 \
            aiohttp[speedups]==3.9.1 \
            beautifulsoup4==4.12.2 \
            lxml==4.9.3 \
            python-docx==0.8.11 \
            python-pptx==0.6.21 \
            PyPDF2==3.0.1 \
            aiofiles==23.2.1 \
            asyncio-throttle==1.0.2

      - name: Download and setup Sudachi
        run: |
          mkdir -p sudachi
          cd sudachi
          
          # Download Sudachi JAR
          wget -q https://github.com/WorksApplications/Sudachi/releases/download/v0.7.3/sudachi-0.7.3-executable.jar
          
          # Download dictionary
          wget -q https://github.com/WorksApplications/SudachiDict/releases/download/v20231110/sudachi-dictionary-20231110-full.zip
          unzip -q sudachi-dictionary-20231110-full.zip
          rm sudachi-dictionary-20231110-full.zip

      - name: Create Sudachi configuration
        run: |
          cat > sudachi/sudachi.json << 'EOF'
          {
            "systemDict": "system_full.dic",
            "characterDefinitionFile": "char.def",
            "inputTextPlugin": [
              {
                "class": "com.worksap.nlp.sudachi.DefaultInputTextPlugin"
              }
            ],
            "oovProviderPlugin": [
              {
                "class": "com.worksap.nlp.sudachi.MeCabOovProviderPlugin",
                "charDef": "char.def",
                "unkDef": "unk.def"
              }
            ],
            "pathRewritePlugin": [
              {
                "class": "com.worksap.nlp.sudachi.JoinNumericPlugin"
              },
              {
                "class": "com.worksap.nlp.sudachi.JoinKatakanaOovPlugin"
              }
            ]
          }
          EOF

      - name: Cache dependencies
        uses: actions/cache@v3
        with:
          path: |
            sudachi/
            ~/.cache/pip/
          key: deps-${{ runner.os }}-${{ hashFiles('**/requirements.txt') }}-${{ hashFiles('sudachi/**') }}

  crawler:
    needs: dependencies
    runs-on: ubuntu-latest
    timeout-minutes: ${{ fromJson(env.MAX_RUNTIME_MINUTES) }}
    strategy:
      fail-fast: false
      matrix:
        shard: [1, 2, 3]
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: 'pip'

      - name: Restore dependencies cache
        uses: actions/cache@v3
        with:
          path: |
            sudachi/
            ~/.cache/pip/
          key: deps-${{ runner.os }}-${{ hashFiles('**/requirements.txt') }}-${{ hashFiles('sudachi/**') }}

      - name: Install Python dependencies
        run: |
          pip install --no-cache-dir \
            supabase==1.0.4 \
            aiohttp[speedups]==3.9.1 \
            beautifulsoup4==4.12.2 \
            lxml==4.9.3 \
            python-docx==0.8.11 \
            python-pptx==0.6.21 \
            PyPDF2==3.0.1 \
            aiofiles==23.2.1 \
            asyncio-throttle==1.0.2

      - name: Create advanced crawler
        run: |
          cat > advanced_crawler.py << 'EOF'
          import asyncio
          import aiohttp
          import hashlib
          import os
          import tempfile
          import json
          import logging
          from urllib.parse import urljoin, urlparse
          from bs4 import BeautifulSoup
          from concurrent.futures import ThreadPoolExecutor
          from supabase import create_client, Client
          import PyPDF2
          from docx import Document
          from pptx import Presentation
          from typing import Set, List, Dict, Optional
          from datetime import datetime
          import time
          import signal
          import sys
          from dataclasses import dataclass
          import re

          # ログ設定
          logging.basicConfig(
              level=logging.INFO,
              format='%(asctime)s - %(levelname)s - %(message)s',
              handlers=[logging.StreamHandler(sys.stdout)]
          )
          logger = logging.getLogger(__name__)

          @dataclass
          class CrawlStats:
              processed: int = 0
              errors: int = 0
              documents_saved: int = 0
              start_time: float = 0
              
              @property
              def runtime(self) -> float:
                  return time.time() - self.start_time if self.start_time else 0
              
              def log_progress(self):
                  rate = self.processed / self.runtime if self.runtime > 0 else 0
                  logger.info(f"Progress: {self.processed} processed, {self.errors} errors, {rate:.2f} docs/sec")

          class ContentExtractor:
              @staticmethod
              async def extract_text(content: bytes, content_type: str, url: str) -> str:
                  loop = asyncio.get_event_loop()
                  
                  try:
                      if 'text/html' in content_type:
                          with ThreadPoolExecutor(max_workers=2) as executor:
                              return await loop.run_in_executor(
                                  executor, ContentExtractor._extract_html, content
                              )
                      elif 'application/pdf' in content_type:
                          with ThreadPoolExecutor(max_workers=2) as executor:
                              return await loop.run_in_executor(
                                  executor, ContentExtractor._extract_pdf, content
                              )
                      elif 'wordprocessingml' in content_type:
                          with ThreadPoolExecutor(max_workers=1) as executor:
                              return await loop.run_in_executor(
                                  executor, ContentExtractor._extract_docx, content
                              )
                      elif 'presentationml' in content_type:
                          with ThreadPoolExecutor(max_workers=1) as executor:
                              return await loop.run_in_executor(
                                  executor, ContentExtractor._extract_pptx, content
                              )
                  except Exception as e:
                      logger.error(f"Extraction error for {url}: {e}")
                  
                  return ""
              
              @staticmethod
              def _extract_html(content: bytes) -> str:
                  try:
                      soup = BeautifulSoup(content, 'lxml')
                      
                      # 不要要素削除
                      for tag in ['script', 'style', 'nav', 'header', 'footer']:
                          for element in soup.find_all(tag):
                              element.decompose()
                      
                      # メインコンテンツ取得
                      main = soup.find('main') or soup.find('article') or soup.find('div', class_=re.compile('content|main'))
                      text = main.get_text(separator=' ', strip=True) if main else soup.get_text(separator=' ', strip=True)
                      
                      return re.sub(r'\s+', ' ', text).strip()
                  except:
                      return ""
              
              @staticmethod
              def _extract_pdf(content: bytes) -> str:
                  try:
                      with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                          tmp.write(content)
                          tmp.flush()
                          
                          text_parts = []
                          with open(tmp.name, 'rb') as file:
                              reader = PyPDF2.PdfReader(file)
                              for i, page in enumerate(reader.pages[:50]):  # 最大50ページ
                                  try:
                                      text_parts.append(page.extract_text())
                                  except:
                                      continue
                          
                          os.unlink(tmp.name)
                          return '\n'.join(text_parts)
                  except:
                      return ""
              
              @staticmethod
              def _extract_docx(content: bytes) -> str:
                  try:
                      with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as tmp:
                          tmp.write(content)
                          tmp.flush()
                          
                          doc = Document(tmp.name)
                          paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
                          
                          os.unlink(tmp.name)
                          return '\n'.join(paragraphs)
                  except:
                      return ""
              
              @staticmethod
              def _extract_pptx(content: bytes) -> str:
                  try:
                      with tempfile.NamedTemporaryFile(suffix='.pptx', delete=False) as tmp:
                          tmp.write(content)
                          tmp.flush()
                          
                          prs = Presentation(tmp.name)
                          text_parts = []
                          for slide in prs.slides:
                              for shape in slide.shapes:
                                  if hasattr(shape, "text"):
                                      text_parts.append(shape.text.strip())
                          
                          os.unlink(tmp.name)
                          return '\n'.join(text_parts)
                  except:
                      return ""

          class MHLWCrawler:
              def __init__(self, shard_id: int = 1, total_shards: int = 3):
                  self.supabase = create_client(
                      os.getenv("SUPABASE_URL"),
                      os.getenv("SUPABASE_ANON_KEY")
                  )
                  
                  self.shard_id = shard_id
                  self.total_shards = total_shards
                  self.crawled_urls: Set[str] = set()
                  self.stats = CrawlStats()
                  self.shutdown = False
                  
                  # シグナルハンドラ
                  signal.signal(signal.SIGINT, self._shutdown_handler)
                  signal.signal(signal.SIGTERM, self._shutdown_handler)
              
              def _shutdown_handler(self, signum, frame):
                  logger.info(f"Received signal {signum}, shutting down gracefully...")
                  self.shutdown = True
              
              async def load_crawled_urls(self):
                  """既存URLの読み込み"""
                  try:
                      result = self.supabase.table('crawled_urls').select('url').execute()
                      self.crawled_urls = {row['url'] for row in result.data}
                      logger.info(f"Loaded {len(self.crawled_urls)} crawled URLs")
                  except Exception as e:
                      logger.error(f"Error loading URLs: {e}")
              
              async def start_crawling(self):
                  """クロール開始"""
                  self.stats.start_time = time.time()
                  await self.load_crawled_urls()
                  
                  # エントリーポイント（シャード分散）
                  entry_points = [
                      "https://www.mhlw.go.jp/",
                      "https://www.mhlw.go.jp/stf/",
                      "https://www.mhlw.go.jp/content/",
                      "https://www.mhlw.go.jp/file/",
                      "https://www.mhlw.go.jp/topics/",
                      "https://www.mhlw.go.jp/houdou/",
                      "https://www.mhlw.go.jp/shingi/",
                      "https://www.mhlw.go.jp/bunya/"
                  ]
                  
                  # シャード分散
                  shard_entries = [url for i, url in enumerate(entry_points) 
                                 if i % self.total_shards == (self.shard_id - 1)]
                  
                  logger.info(f"Shard {self.shard_id}: Processing {len(shard_entries)} entry points")
                  
                  connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
                  timeout = aiohttp.ClientTimeout(total=60)
                  
                  async with aiohttp.ClientSession(
                      connector=connector,
                      timeout=timeout,
                      headers={'User-Agent': 'Mozilla/5.0 (compatible; MedicalBot/1.0)'}
                  ) as session:
                      
                      tasks = []
                      semaphore = asyncio.Semaphore(10)
                      
                      for entry_url in shard_entries:
                          task = self.crawl_section

### 私のプロジェクトに commitできる？


申し訳ございませんが、私はGitHubへの直接的なcommitやpushを行うことはできません。私にはそのような権限がありません。

ただし、以下の方法でお手伝いできます：

## 1. コードファイルの提供
```bash
# 以下のようなファイル構成でコードを提供できます：
.github/workflows/medical-term-crawler.yml
src/advanced_crawler.py
src/morphological_analyzer.py
src/database_handler.py
src/content_extractor.py
requirements.txt
README.md
```

## 2. 完全なファイルセットを一括作成
必要な全ファイルを一度に提供し、あなたがコピー&ペーストでプロジェクトに追加できるようにします。

## 3. GitHub Actions設定のガイド
Secretsの設定方法や実行手順を詳しく説明します。

どちらの方法をご希望でしょうか？

また、プロジェクトの具体的な構成（ディレクトリ構造、既存ファイルなど）を教えていただければ、それに合わせてコードを調整できます。

例えば：
- プロジェクトのルートディレクトリ構成
- 既存のPythonパッケージ構造
- 設定ファイルの配置場所
- 使用したいブランチ戦略

これらの情報があれば、より適切なコード構成を提案できます。

### project名をmtcにした。これを git repositoryにしたい。gh commandでどうするんだっけ？


```bash
# プロジェクトディレクトリに移動
cd mtc

# Gitリポジトリを初期化
git init

# .gitignoreファイルを作成
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Project specific
crawler.log
*.tmp
temp/
sudachi/
analysis_results.json
crawled_content.txt
new_terms.txt

# Secrets (just in case)
.env
config.json
EOF

# 初回コミット用のファイルを追加
echo "# Medical Term Crawler (MTC)" > README.md
echo "" >> README.md
echo "厚労省サイトから医療用語を自動収集・解析するシステム" >> README.md

# ファイルをステージング
git add .

# 初回コミット
git commit -m "Initial commit: Setup MTC project structure"

# GitHub CLIでリモートリポジトリを作成（プライベート）
gh repo create mtc --private --description "Medical Term Crawler - 医療用語自動収集システム"

# リモートリポジトリにプッシュ
git push -u origin main

# または、パブリックリポジトリにしたい場合：
# gh repo create mtc --public --description "Medical Term Crawler - 医療用語自動収集システム"
```

リポジトリ作成後の基本的なセットアップ：

```bash
# GitHub Secretsを設定（必須）
gh secret set SUPABASE_URL --body "your_supabase_project_url"
gh secret set SUPABASE_ANON_KEY --body "your_supabase_anon_key"

# リポジトリの情報を確認
gh repo view

# ブランチ保護ルールを設定（オプション）
gh api repos/:owner/:repo/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":[]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1}' \
  --field restrictions=null
```

基本的なプロジェクト構造を作成：

```bash
# ディレクトリ構造を作成
mkdir -p .github/workflows
mkdir -p src
mkdir -p tests
mkdir -p docs

# 基本ファイルを作成
touch src/__init__.py
touch tests/__init__.py
touch requirements.txt
```

これで`mtc`という名前のGitHubリポジトリが作成され、ローカルとリモートが連携されます。

### src/advanced_crawler.py とするなら、上記 workflow内に記述不要では？

その通りです！完全に正しい指摘です。

ファイルを別途作成する場合、workflowは以下のようにシンプルになります：

```yaml
name: Advanced Medical Term Crawler

on:
  schedule:
    - cron: '0 2 * * *'
  workflow_dispatch:
    inputs:
      max_workers:
        description: 'Maximum number of worker threads'
        required: false
        default: '15'
      max_depth:
        description: 'Maximum crawl depth'
        required: false
        default: '4'

jobs:
  crawler:
    runs-on: ubuntu-latest
    timeout-minutes: 300
    strategy:
      fail-fast: false
      matrix:
        shard: [1, 2, 3]
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          sudo apt-get update
          sudo apt-get install -y libreoffice-headless poppler-utils

      - name: Setup Sudachi
        run: |
          ./scripts/setup_sudachi.sh

      - name: Run crawler (Shard ${{ matrix.shard }})
        run: |
          python src/advanced_crawler.py --shard ${{ matrix.shard }} --total-shards 3
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
          MAX_WORKERS: ${{ github.event.inputs.max_workers || '15' }}
          MAX_DEPTH: ${{ github.event.inputs.max_depth || '4' }}

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: crawler-results-shard-${{ matrix.shard }}
          path: |
            logs/
            results/

  analyzer:
    needs: crawler
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python and Java
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Setup Java
        uses: actions/setup-java@v4
        with:
          java-version: '11'
          distribution: 'temurin'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Download artifacts
        uses: actions/download-artifact@v4
        with:
          pattern: crawler-results-shard-*
          merge-multiple: true

      - name: Run morphological analysis
        run: |
          python src/morphological_analyzer.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}

      - name: Run term validation
        run: |
          python src/database_handler.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
```

これでworkflowは本来の役割（実行制御）に集中でき、コードは適切にファイル分離されます。

プロジェクト構成は：
```
mtc/
├── .github/workflows/medical-term-crawler.yml
├── src/
│   ├── advanced_crawler.py
│   ├── morphological_analyzer.py
│   ├── database_handler.py
│   └── content_extractor.py
├── scripts/setup_sudachi.sh
├── requirements.txt
└── README.md
```

この方がメンテナンスしやすく、テストも書きやすくなります。

### 


```bash
# プロジェクトディレクトリ作成
mkdir mtc
cd mtc

# Git初期化
git init

# ディレクトリ構造作成
mkdir -p .github/workflows
mkdir -p src
mkdir -p scripts
mkdir -p tests
mkdir -p docs

# .gitignore作成
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
.pytest_cache/

# Virtual environments
venv/
env/
ENV/
.venv/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Project specific
logs/
results/
temp/
sudachi/
*.log
*.tmp
analysis_results.json
crawled_content.txt
new_terms.txt

# Secrets
.env
config.json
secrets.yaml
EOF

# requirements.txt作成
cat > requirements.txt << 'EOF'
supabase==1.0.4
aiohttp[speedups]==3.9.1
beautifulsoup4==4.12.2
lxml==4.9.3
python-docx==0.8.11
python-pptx==0.6.21
PyPDF2==3.0.1
aiofiles==23.2.1
asyncio-throttle==1.0.2
EOF

# README.md作成
cat > README.md << 'EOF'
# Medical Term Crawler (MTC)

厚労省サイトから医療用語を自動収集・解析するシステム

## 概要
- 厚生労働省のWebサイトを定期的にクロール
- 日本語形態素解析（Sudachi）による専門用語抽出
- 新規医療用語の自動発見
- Supabaseによるデータ管理

## セットアップ

### 1. 環境変数設定
GitHub Secretsに以下を設定:
```
SUPABASE_URL=your_supabase_project_url
SUPABASE_ANON_KEY=your_supabase_anon_key
```

### 2. 手動実行
```bash
gh workflow run medical-term-crawler.yml
```

## アーキテクチャ
- 並列クロール（シャード分散）
- 非同期処理による高速化
- PDF/DOCX/PPTX対応
- 重複回避機構

## ライセンス
MIT
EOF

# GitHub workflow作成
cat > .github/workflows/medical-term-crawler.yml << 'EOF'
name: Medical Term Crawler

on:
  schedule:
    - cron: '0 2 * * *'
  workflow_dispatch:
    inputs:
      max_workers:
        description: 'Maximum worker threads'
        required: false
        default: '15'
      max_depth:
        description: 'Maximum crawl depth'
        required: false
        default: '4'

jobs:
  crawler:
    runs-on: ubuntu-latest
    timeout-minutes: 300
    strategy:
      fail-fast: false
      matrix:
        shard: [1, 2, 3]
    
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          cache: 'pip'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          sudo apt-get update -qq
          sudo apt-get install -y libreoffice-headless poppler-utils
      
      - name: Setup Sudachi
        run: |
          chmod +x scripts/setup_sudachi.sh
          ./scripts/setup_sudachi.sh
      
      - name: Run crawler
        run: |
          python src/advanced_crawler.py \
            --shard ${{ matrix.shard }} \
            --total-shards 3 \
            --max-workers ${{ github.event.inputs.max_workers || '15' }} \
            --max-depth ${{ github.event.inputs.max_depth || '4' }}
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
      
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: results-shard-${{ matrix.shard }}
          path: results/

  analyzer:
    needs: crawler
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - uses: actions/setup-java@v4
        with:
          java-version: '11'
          distribution: 'temurin'
      
      - run: pip install -r requirements.txt
      
      - name: Setup Sudachi
        run: |
          chmod +x scripts/setup_sudachi.sh
          ./scripts/setup_sudachi.sh
      
      - uses: actions/download-artifact@v4
        with:
          pattern: results-shard-*
          merge-multiple: true
      
      - name: Analyze terms
        run: python src/morphological_analyzer.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
      
      - name: Update database
        run: python src/database_handler.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
EOF

# Sudachiセットアップスクリプト
cat > scripts/setup_sudachi.sh << 'EOF'
#!/bin/bash
set -e

SUDACHI_VERSION="0.7.3"
DICT_VERSION="20231110"

mkdir -p sudachi
cd sudachi

# Download Sudachi
if [ ! -f "sudachi-${SUDACHI_VERSION}-executable.jar" ]; then
    wget -q https://github.com/WorksApplications/Sudachi/releases/download/v${SUDACHI_VERSION}/sudachi-${SUDACHI_VERSION}-executable.jar
fi

# Download dictionary
if [ ! -f "system_full.dic" ]; then
    wget -q https://github.com/WorksApplications/SudachiDict/releases/download/v${DICT_VERSION}/sudachi-dictionary-${DICT_VERSION}-full.zip
    unzip -q sudachi-dictionary-${DICT_VERSION}-full.zip
    rm sudachi-dictionary-${DICT_VERSION}-full.zip
fi

# Create config
cat > sudachi.json << 'JSON'
{
  "systemDict": "system_full.dic",
  "characterDefinitionFile": "char.def",
  "inputTextPlugin": [
    {"class": "com.worksap.nlp.sudachi.DefaultInputTextPlugin"}
  ],
  "oovProviderPlugin": [
    {
      "class": "com.worksap.nlp.sudachi.MeCabOovProviderPlugin",
      "charDef": "char.def",
      "unkDef": "unk.def"
    }
  ],
  "pathRewritePlugin": [
    {"class": "com.worksap.nlp.sudachi.JoinNumericPlugin"},
    {"class": "com.worksap.nlp.sudachi.JoinKatakanaOovPlugin"}
  ]
}
JSON

cd ..
EOF

chmod +x scripts/setup_sudachi.sh

# src/__init__.py
touch src/__init__.py

# 初期コミット
git add .
git commit -m "Initial commit: Setup MTC project structure"

# GitHubリポジトリ作成
gh repo create mtc --private --description "Medical Term Crawler - 医療用語自動収集システム"

# プッシュ
git push -u origin main

# Secrets設定
echo "以下のコマンドでSecretsを設定してください："
echo "gh secret set SUPABASE_URL --body \"your_supabase_url\""
echo "gh secret set SUPABASE_ANON_KEY --body \"your_supabase_anon_key\""
```

これでプロジェクトの基本構造が完成します。次はPythonソースファイル（src/advanced_crawler.py等）を作成しますか？

### advanced_crawler.py を作成して下さい。


```python
# src/advanced_crawler.py
import asyncio
import aiohttp
import aiofiles
import hashlib
import os
import tempfile
import json
import logging
import argparse
import signal
import sys
from urllib.parse import urljoin, urlparse, quote
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from supabase import create_client, Client
import PyPDF2
from docx import Document
from pptx import Presentation
from typing import Set, List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import time
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
import random

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlJob:
    """クロールジョブ"""
    url: str
    depth: int
    priority: int = 0
    parent_url: str = ""
    retry_count: int = 0
    discovered_at: float = field(default_factory=time.time)
    
    def __lt__(self, other):
        return (self.priority, self.discovered_at) < (other.priority, other.discovered_at)

@dataclass
class CrawlStats:
    """クロール統計"""
    start_time: float = field(default_factory=time.time)
    processed: int = 0
    errors: int = 0
    documents_saved: int = 0
    links_discovered: int = 0
    retries: int = 0
    skipped: int = 0
    
    @property
    def runtime(self) -> float:
        return time.time() - self.start_time
    
    @property
    def rate(self) -> float:
        return self.processed / self.runtime if self.runtime > 0 else 0
    
    def log_progress(self):
        """進捗をログ出力"""
        logger.info(
            f"Progress: {self.processed} processed, {self.errors} errors, "
            f"{self.documents_saved} saved, {self.rate:.2f} docs/sec, "
            f"Runtime: {self.runtime/60:.1f} min"
        )

class ContentExtractor:
    """コンテンツ抽出クラス"""
    
    @staticmethod
    async def extract_text(content: bytes, content_type: str, url: str) -> str:
        """非同期でテキストを抽出"""
        loop = asyncio.get_event_loop()
        
        try:
            if 'text/html' in content_type:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_html, content
                    )
            elif 'application/pdf' in content_type:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_pdf, content
                    )
            elif any(ct in content_type for ct in ['wordprocessingml', 'msword']):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_docx, content
                    )
            elif any(ct in content_type for ct in ['presentationml', 'ms-powerpoint']):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    return await loop.run_in_executor(
                        executor, ContentExtractor._extract_pptx, content
                    )
        except Exception as e:
            logger.error(f"Text extraction error for {url}: {e}")
        
        return ""
    
    @staticmethod
    def _extract_html(content: bytes) -> str:
        """HTMLからテキスト抽出"""
        try:
            # エンコーディング検出
            encoding = 'utf-8'
            if b'charset=' in content[:1024]:
                match = re.search(rb'charset=(["\']?)([^"\'>]+)', content[:1024])
                if match:
                    encoding = match.group(2).decode('ascii', errors='ignore')
            
            html = content.decode(encoding, errors='ignore')
            soup = BeautifulSoup(html, 'lxml')
            
            # 不要要素削除
            for tag in ['script', 'style', 'nav', 'header', 'footer', 'aside', 'form']:
                for element in soup.find_all(tag):
                    element.decompose()
            
            # メインコンテンツ取得
            main_content = None
            for selector in ['main', 'article', '.main-content', '#content']:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if main_content:
                text = main_content.get_text(separator='\n', strip=True)
            else:
                text = soup.get_text(separator='\n', strip=True)
            
            # テキスト正規化
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            
            return text.strip()
        except Exception as e:
            logger.debug(f"HTML extraction error: {e}")
            return ""
    
    @staticmethod
    def _extract_pdf(content: bytes) -> str:
        """PDFからテキスト抽出"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                text_parts = []
                with open(tmp_file.name, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    max_pages = min(len(reader.pages), 100)
                    
                    for page_num in range(max_pages):
                        try:
                            page = reader.pages[page_num]
                            page_text = page.extract_text()
                            if page_text:
                                text_parts.append(page_text)
                        except Exception:
                            continue
                
                os.unlink(tmp_file.name)
                return '\n'.join(text_parts)
        except Exception as e:
            logger.debug(f"PDF extraction error: {e}")
            return ""
    
    @staticmethod
    def _extract_docx(content: bytes) -> str:
        """DOCXからテキスト抽出"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                doc = Document(tmp_file.name)
                paragraphs = []
                
                for paragraph in doc.paragraphs:
                    if paragraph.text.strip():
                        paragraphs.append(paragraph.text.strip())
                
                # テーブルからも抽出
                for table in doc.tables:
                    for row in table.rows:
                        row_text = [cell.text.strip() for cell in row.cells if cell.text.strip()]
                        if row_text:
                            paragraphs.append(' | '.join(row_text))
                
                os.unlink(tmp_file.name)
                return '\n'.join(paragraphs)
        except Exception as e:
            logger.debug(f"DOCX extraction error: {e}")
            return ""
    
    @staticmethod
    def _extract_pptx(content: bytes) -> str:
        """PPTXからテキスト抽出"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.pptx', delete=False) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                
                prs = Presentation(tmp_file.name)
                text_parts = []
                
                for slide_num, slide in enumerate(prs.slides):
                    slide_texts = []
                    for shape in slide.shapes:
                        if hasattr(shape, "text") and shape.text:
                            slide_texts.append(shape.text.strip())
                    
                    if slide_texts:
                        text_parts.append(f"\n--- Slide {slide_num + 1} ---\n")
                        text_parts.extend(slide_texts)
                
                os.unlink(tmp_file.name)
                return '\n'.join(text_parts)
        except Exception as e:
            logger.debug(f"PPTX extraction error: {e}")
            return ""

class MHLWCrawler:
    """厚労省サイトクローラー"""
    
    def __init__(self, shard_id: int = 1, total_shards: int = 1, 
                 max_workers: int = 15, max_depth: int = 4):
        # 設定
        self.shard_id = shard_id
        self.total_shards = total_shards
        self.max_workers = max_workers
        self.max_depth = max_depth
        self.base_url = "https://www.mhlw.go.jp"
        
        # Supabase
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_ANON_KEY")
        )
        
        # 状態管理
        self.crawled_urls: Set[str] = set()
        self.processing_urls: Set[str] = set()
        self.failed_urls: Dict[str, int] = {}
        self.url_queue = asyncio.PriorityQueue(maxsize=5000)
        
        # 制御
        self.semaphore = asyncio.Semaphore(max_workers)
        self.download_semaphore = asyncio.Semaphore(max(max_workers // 2, 1))
        self.shutdown_event = asyncio.Event()
        
        # 統計
        self.stats = CrawlStats()
        
        # robots.txt
        self.robots_parser = None
        
        # レート制限
        self.domain_delays = {}
        self.min_delay = 0.5
        self.max_delay = 2.0
        
        # 出力ディレクトリ
        os.makedirs('logs', exist_ok=True)
        os.makedirs('results', exist_ok=True)
        
        # シグナルハンドラ
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """シグナルハンドラ"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_event.set()
    
    async def initialize(self):
        """初期化"""
        logger.info(f"Initializing crawler (shard {self.shard_id}/{self.total_shards})...")
        
        # 既存URL読み込み
        await self._load_crawled_urls()
        
        # robots.txt読み込み
        await self._load_robots_txt()
        
        logger.info(f"Initialization complete. {len(self.crawled_urls)} URLs already crawled.")
    
    async def _load_crawled_urls(self):
        """クロール済みURL読み込み"""
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.supabase.table('crawled_urls').select('url').execute()
            )
            self.crawled_urls = {row['url'] for row in result.data}
        except Exception as e:
            logger.error(f"Error loading crawled URLs: {e}")
    
    async def _load_robots_txt(self):
        """robots.txt読み込み"""
        try:
            self.robots_parser = RobotFileParser()
            self.robots_parser.set_url(f"{self.base_url}/robots.txt")
            await asyncio.get_event_loop().run_in_executor(
                None, self.robots_parser.read
            )
        except Exception as e:
            logger.warning(f"Could not load robots.txt: {e}")
    
    def can_fetch(self, url: str) -> bool:
        """robots.txtチェック"""
        if not self.robots_parser:
            return True
        try:
            return self.robots_parser.can_fetch("*", url)
        except:
            return True
    
    async def start_crawling(self):
        """クロール開始"""
        await self.initialize()
        
        logger.info("Starting crawl...")
        
        # エントリーポイント
        entry_points = [
            (f"{self.base_url}/", 0, 1),
            (f"{self.base_url}/stf/", 0, 2),
            (f"{self.base_url}/stf/seisakunitsuite/bunya/kenkou_iryou/", 0, 3),
            (f"{self.base_url}/content/", 0, 2),
            (f"{self.base_url}/file/", 0, 2),
            (f"{self.base_url}/topics/", 0, 2),
            (f"{self.base_url}/houdou/", 0, 2),
            (f"{self.base_url}/shingi/", 0, 2),
            (f"{self.base_url}/bunya/", 0, 2),
        ]
        
        # シャード分散
        for i, (url, depth, priority) in enumerate(entry_points):
            if i % self.total_shards == (self.shard_id - 1):
                await self.url_queue.put(CrawlJob(url, depth, priority))
        
        # ワーカー起動
        workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker(f"Worker-{i}"))
            workers.append(worker)
        
        # 統計レポータ
        stats_task = asyncio.create_task(self._stats_reporter())
        
        try:
            # ワーカー完了待機
            await asyncio.gather(*workers)
        finally:
            stats_task.cancel()
            await self._save_results()
        
        logger.info(f"Crawl completed. {self.stats.processed} URLs processed.")
    
    async def _worker(self, name: str):
        """ワーカータスク"""
        logger.debug(f"{name} started")
        
        connector = aiohttp.TCPConnector(
            limit=30,
            limit_per_host=5,
            ttl_dns_cache=300
        )
        
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (compatible; MedicalTermBot/1.0)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ja,en;q=0.9'
            }
        ) as session:

