from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:1234/v1",
    api_key="lm-studio"
)

def generate_updates(changes, dependencies):
    prompt = f"""
You are a document update assistant.

Changes:
{changes}

Dependencies:
{dependencies}

Generate updated content ONLY for affected sections.
Return clean JSON.
"""

    response = client.chat.completions.create(
        model="qwen2.5-7b-instruct",
        messages=[
            {"role": "system", "content": "You generate structured document updates."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3
    )

    return response.choices[0].message.content