import os
import requests
from urllib3.exceptions import InsecureRequestWarning
import json

class MistralAnalyzer:
    def __init__(self):
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        self.api_key = os.getenv('MISTRAL_API_KEY')
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.url = "https://api.mistral.ai/v1/chat/completions"
        self.model = "mistral-small-latest"

    def analyze_error(self, error_details):
        prompt = f"""As an expert cloud systems analyst, analyze this error and 
        provide a concise response in markdown format:

        Error: {error_details}

        Format your response as:
        • **Root Cause:** (one line)
        • **Solution:** Concise, step by step solution (one line per step)

        Keep each section under 1500 characters."""

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "top_p": 1,
            "max_tokens": 1000,
            "stream": False,
            "safe_prompt": True
        }

        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json=payload,
                verify=False
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            else:
                print(f"API Error: {response.status_code} - {response.text}")
                return f"Error: API returned status code {response.status_code}"
                
        except Exception as e:
            print(f"Request Error: {str(e)}")
            return f"Error getting AI analysis: {str(e)}"

    def get_detailed_analysis(self, error_details):
        return f"""Provide detailed analysis including:
        1. Root cause analysis
        2. Multiple solution options
        3. Prevention measures
        
        Error: {error_details}"""
