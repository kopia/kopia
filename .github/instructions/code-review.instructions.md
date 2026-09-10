---
description: General Code Review Instructions
excludeAgent: "cloud-agent"

---

# Code Review Instructions

## Review Style

- Be concise and avoid filler phrases and unnecessary or superfluous adjectives and adverbs
- Provide direct, brief, specific, actionable feedback
- Explain the rationale behind recommendations
- Ask clarifying questions when code intent is unclear

## Focus

- Prioritize correctness, security vulnerabilities and performance issues
- Always suggest changes to improve maintainability

## Code Quality Essentials

- Functions should be focused and appropriately sized
- Use clear, descriptive naming conventions
- Ensure proper error handling throughout

## Security Critical Issues

- Check for hardcoded secrets, API keys, or credentials
- Verify proper input validation and sanitization
- Review authentication and authorization logic

## Performance Issues

- Spot inefficient loops and algorithmic issues
- Check for memory leaks and resource cleanup
- Review caching opportunities for expensive operations

## Review Test Coverage

- Ensure there are tests that cover and exercise the new or changed functionality
