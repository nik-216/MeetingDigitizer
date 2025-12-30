# M²-DSUM- Multimodal Meeting Digitization and Summarization
Meetings often involve rich and fast-paced discussions, speech, slides, shared screens, and whiteboard visuals, making it difficult for a single note-taker to manually capture everything accurately. This manual process is time-consuming, prone to human bias, and often results in missing key details, action items, or context especially in long or information-dense sessions. M²-DSUM addresses this challenge by automatically transforming multimodal meeting recordings into concise and structured summaries. Leveraging speech recognition, OCR, computer vision, and Large Language Models, the system ingests and temporally aligns audio, text, and visual modalities using an Apache Kafka–based pipeline. This enables summaries that maintain context and coherence while significantly improving information completeness (ROUGE-L: 0.622). Designed for organizations, remote and hybrid teams, students, educators, and anyone who depends on accurate meeting documentation, M²-DSUM eliminates the need for manual note-taking and makes meeting knowledge searchable, accessible, and easier to review and store.

# Architecture diagram
<img width="1234" height="378" alt="image" src="https://github.com/user-attachments/assets/e6c951e1-4799-47d9-83e0-acbd36a3629f" />

The architecture diagram illustrates the end-to-end data flow of the M²-DSUM system starting from raw meeting videos and moving through multimodal extraction, processing, and AI-based summarization. Each stage in the pipeline is designed to handle a specific modality—audio, text, slides, and visuals while Apache Kafka ensures synchronized near real-time streaming between components. The result is a coherent, structured summary that brings together all relevant meeting information in one unified output.

# Key features of the system

**1. Multimodal Data Ingestion —** Captures and streams audio, speaker video, slides, and whiteboard content through an Apache Kafka pipeline.

**2. Automated Speech-to-Text Processing —** Converts meeting speech into accurate transcripts using AI-based ASR models.

**3. Visual Content Extraction —** Uses computer vision, OCR, and a **custom diagram detection module** to extract not only text but also diagrammatic information from slides and hand drawn visuals.

**4. Context-Aware Summarization —** LLM-based engine generates coherent summaries, and key points that preserve semantic context across modalities.

**5. Temporal Alignment Engine — **Synchronizes audio, text, and visual streams to ensure summarized content reflects the correct sequence of discussion.

**6. Searchable & Exportable Output — **Produces meeting summaries that can be stored, shared, or integrated into dashboards for later review.



## Tech Stack

| Module          | Technology used                                 | Purpose                                                                 |
|---------------------------|--------------------------------------------------|-------------------------------------------------------------------------|
| Data Ingestion | **Apache Kafka**, **FFmpeg**                     | Streams audio/video and extracts frames.                  |
| Audio Processing Consumer          | **Whisper**, **Wav2Vec2**, **Rezemblyzer**                        | Converts spoken content into text                                       |
| Diagram Processing Consumer          | **YOLOv8**, **PaddleOCR**, **OpenCV** | Detects diagrams from slides/whiteboards              |
| Text Processing Consumer    | **PaddleOCR**            | Detectes text from whiteboards and slides            |
| Summarization Consumer       | **Large Language Models (LLMs)**                 | Generates structured summaries, and key points discussed in the meeting             |
| Pipeline Logic   | **Python**                                       | Coordinates modules and executes processing pipeline                    |
| Deployment   | **Docker**                           | Each consumer runs in a separate container for isolation, scalability, and modularity                   |
| Interface    | **Tkinter (Desktop UI)**, **XLaunch (Live Screen Stream)**                     | Displays live processing results (text extraction, diagrams, ASR feed)          |


## How to Run

### 1. Clone the Repository
```bash
git clone https://github.com/nik-216/MeetingDigitizer.git
cd m2-dsum
```
### 2. Start Kafka and all the modules
```bash
docker-compose up --build
```


## Sample Output
**Input**

Link to video - https://drive.google.com/file/d/1XiUdOp3DnKYJHzmV53_2T7M-BJrN6j4k/view?usp=drive_link


**Output meeting summary**

Variables are fundamentally categorized as either quantitative or qualitative. Quantitative variables are numerically measurable,
yielding quantitative data, which can be further differentiated into continuous and discrete types. Continuous data can assume any
numerical value within a given range, offering infinite possibilities (e.g., length), whereas discrete data consists of specific, countable values with distinct jumps (e.g., number of students). In contrast, qualitative or categorical variables classify data into non-numeric categories, even when numerically coded for convenience (e.g., gender), resulting in qualitative data such as descriptions of ”great
fun.”









