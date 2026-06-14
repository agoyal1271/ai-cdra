/**
 * Semantic PII Detection using embeddings
 *
 * Instead of exact pattern matching (email, phone, ssn),
 * use embeddings to find semantic similarities.
 * Catches variations like: phone_number, contactPhone, phoneNum, etc.
 */

// Known PII patterns (seed phrases)
const PII_PATTERNS = [
  // Identity
  'social security number',
  'ssn',
  'passport',
  'national id',
  'driver license',
  'identification number',

  // Contact
  'email address',
  'email',
  'phone number',
  'phone',
  'telephone',
  'mobile',
  'cell phone',
  'contact number',

  // Financial
  'credit card',
  'bank account',
  'routing number',
  'swift code',
  'iban',
  'card number',

  // Personal
  'date of birth',
  'dob',
  'birth date',
  'age',
  'name',
  'first name',
  'last name',
  'full name',
  'address',
  'home address',
  'street address',
  'zip code',
  'postal code',

  // Health
  'health insurance',
  'medical record',
  'patient id',
  'diagnosis',
  'prescription',

  // Employer
  'employee id',
  'member id',
  'employee number',
  'payroll',
  'salary',
];

/**
 * Simple cosine similarity between two vectors
 */
function cosineSimilarity(a: number[], b: number[]): number {
  const dotProduct = a.reduce((sum, val, i) => sum + val * b[i], 0);
  const magnitudeA = Math.sqrt(a.reduce((sum, val) => sum + val * val, 0));
  const magnitudeB = Math.sqrt(b.reduce((sum, val) => sum + val * val, 0));
  return magnitudeA && magnitudeB ? dotProduct / (magnitudeA * magnitudeB) : 0;
}

/**
 * Cache for embeddings (avoid re-computing)
 */
const embeddingCache = new Map<string, number[]>();

/**
 * Get embedding for a text string using the backend embeddings API
 */
async function getEmbedding(text: string): Promise<number[]> {
  const cacheKey = text.toLowerCase();

  if (embeddingCache.has(cacheKey)) {
    return embeddingCache.get(cacheKey)!;
  }

  try {
    // Call backend embeddings endpoint (uses Ollama)
    const response = await fetch('/api/embeddings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });

    if (!response.ok) {
      console.warn(`Embedding failed for "${text}": ${response.status}`);
      return [];
    }

    const data = await response.json();
    const embedding = data.embedding || [];

    embeddingCache.set(cacheKey, embedding);
    return embedding;
  } catch (err) {
    console.warn(`Embedding error for "${text}":`, err);
    return [];
  }
}

/**
 * Detect if a field name is PII using semantic similarity
 * Returns { isPii: boolean, confidence: number, match: string }
 */
export async function detectPiiSemantic(
  fieldName: string,
  threshold: number = 0.7
): Promise<{ isPii: boolean; confidence: number; match: string }> {
  const fieldEmbedding = await getEmbedding(fieldName);

  if (fieldEmbedding.length === 0) {
    // Fallback to exact pattern match if embeddings fail
    return detectPiiExact(fieldName);
  }

  let maxSimilarity = 0;
  let bestMatch = '';

  // Compare against all known PII patterns
  for (const pattern of PII_PATTERNS) {
    const patternEmbedding = await getEmbedding(pattern);
    if (patternEmbedding.length === 0) continue;

    const similarity = cosineSimilarity(fieldEmbedding, patternEmbedding);
    if (similarity > maxSimilarity) {
      maxSimilarity = similarity;
      bestMatch = pattern;
    }
  }

  return {
    isPii: maxSimilarity >= threshold,
    confidence: Math.round(maxSimilarity * 100) / 100,
    match: bestMatch,
  };
}

/**
 * Fallback: exact pattern matching (for when embeddings unavailable)
 */
function detectPiiExact(
  fieldName: string
): { isPii: boolean; confidence: number; match: string } {
  const fieldLower = fieldName.toLowerCase();

  for (const pattern of PII_PATTERNS) {
    if (fieldLower.includes(pattern) || pattern.includes(fieldLower)) {
      return {
        isPii: true,
        confidence: 1.0,
        match: pattern,
      };
    }
  }

  return {
    isPii: false,
    confidence: 0,
    match: '',
  };
}

/**
 * Batch detect PII for multiple fields (parallel)
 */
export async function detectPiiFields(
  fields: Array<{ name: string; type?: string }>,
  threshold: number = 0.7
): Promise<Array<{ field: string; isPii: boolean; confidence: number; match: string }>> {
  const results = await Promise.all(
    fields.map(async (f) => ({
      field: f.name,
      ...(await detectPiiSemantic(f.name, threshold)),
    }))
  );

  return results;
}
