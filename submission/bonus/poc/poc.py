"""
Proof-of-Concept (PoC) for LLM Observability PII Tokenization & Masking.
Topic A: Redacting PII at ingestion before human viewing.

This script demonstrates:
1. Simulating incoming LLM call logs containing sensitive PII (emails, phone numbers, API keys).
2. Tokenizing and redacting PII in-memory.
3. Appending the redacted logs into a local Delta Table representing the Silver layer.
4. Verifying that no PII remains in the Silver table.
"""
from __future__ import annotations

import re
import os
import shutil
import polars as pl
from deltalake import DeltaTable, write_deltalake

# Standard Regex patterns for PII detection
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
PHONE_REGEX = r"\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}"
API_KEY_REGEX = r"(?:sk-[a-zA-Z0-9]{32,48}|AIzaSy[a-zA-Z0-9-_]{33})"

def tokenize_and_redact(text: str) -> str:
    """Mask sensitive PII with tokens (e.g. [REDACTED_EMAIL]) to preserve compliance."""
    if not text:
        return text
    
    # Redact email addresses
    text = re.sub(EMAIL_REGEX, "[REDACTED_EMAIL]", text)
    # Redact phone numbers
    text = re.sub(PHONE_REGEX, "[REDACTED_PHONE]", text)
    # Redact API Keys
    text = re.sub(API_KEY_REGEX, "[REDACTED_API_KEY]", text)
    
    return text

def main():
    print("--- Running PII Tokenization PoC ---")
    
    # 1. Setup paths
    silver_poc_path = os.path.join("submission", "bonus", "poc", "silver_calls_poc")
    if os.path.exists(silver_poc_path):
        shutil.rmtree(silver_poc_path)
    
    # 2. Simulated Bronze Landing payload containing PII
    raw_payloads = [
        {
            "request_id": "req-1",
            "prompt": "Hello, my email is alice@company.com and my API key is sk-1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t",
            "response": "Sure, I have updated your account information. Let me know if you need anything else.",
            "tenant_id": "tenant-abc"
        },
        {
            "prompt": "Call me back at +84 912 345 678. I am using credentials AIzaSyA1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6Q",
            "request_id": "req-2",
            "response": "We will contact you shortly.",
            "tenant_id": "tenant-xyz"
        }
    ]
    
    print("\nSimulated Raw Incoming Data:")
    for record in raw_payloads:
        print(f"  Request ID: {record['request_id']}")
        print(f"    Prompt: {record['prompt']}")
        
    # 3. Apply Tokenization (simulated Spark/Polars transformation)
    processed_records = []
    for r in raw_payloads:
        processed_records.append({
            "request_id": r["request_id"],
            "prompt": tokenize_and_redact(r["prompt"]),
            "response": tokenize_and_redact(r["response"]),
            "tenant_id": r["tenant_id"]
        })
        
    # 4. Write to Silver Delta Table
    df = pl.DataFrame(processed_records)
    print(f"\nWriting tokenized data to Silver Delta Table: {silver_poc_path}")
    write_deltalake(silver_poc_path, df.to_arrow(), mode="overwrite")
    
    # 5. Read back from Delta Table and verify
    dt = DeltaTable(silver_poc_path)
    read_df = pl.from_arrow(dt.to_pyarrow_table())
    
    print("\nRead from Silver Delta Table:")
    print(read_df)
    
    # Assertions
    for row in read_df.iter_rows(named=True):
        prompt = row["prompt"]
        assert "alice@company.com" not in prompt, "PII Email leaked!"
        assert "+84 912 345 678" not in prompt, "PII Phone leaked!"
        assert "sk-" not in prompt, "PII API Key leaked!"
        assert "AIzaSy" not in prompt, "PII API Key leaked!"
        
    print("\n✓ Verification Success: All PII successfully tokenized and redacted.")

if __name__ == "__main__":
    main()
