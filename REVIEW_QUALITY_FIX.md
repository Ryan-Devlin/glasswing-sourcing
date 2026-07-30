# Review Quality Fix

## Needs Review gate

A company now enters Needs Review only when:

- its score is at least 55, and
- it has at least two enterprise evidence dimensions or a clear enterprise signal.

Potential comps and manual overrides remain reviewable regardless of score.

## Low-signal outcomes

Readable sites below the gate are classified as:

- `no_clear_enterprise_or_glasswing_fit`

Blocked, blank, 404, server-error, or effectively empty sites are classified as:

- `website_unavailable_or_no_usable_company_evidence`

These rows remain visible under **Insufficient Evidence** with a specific
reason. This is an uncertainty state, not an investment rejection.

## Hard Denied

Hard Denied is reserved for a factual disqualifier such as a parked domain,
confirmed non-US location, clear consumer product, agency/services business,
non-English page, media property, or commodity wrapper. Low score, sparse
copy, failed fetching, and incomplete evidence cannot cause a hard denial.

## Glasswing enterprise rescue

Readable companies with a strong Glasswing-specific enterprise concept can
enter Needs Review even below the normal score floor. The rescue requires a
real product, enterprise buyer or workflow, and substantive technical or
high-value workflow evidence. It never promotes directly to Investable, and
explicit disqualifiers always take precedence.

## Glasswing fit score

Scoring uses title and description first, falling back to body copy only when
those fields are sparse. Concrete thesis-specific evidence receives more
weight when it intersects with a real buyer, product, technical approach,
workflow, or important problem. Generic AI language and service-business or
low-value patterns receive penalties and score caps.

## Review controls

The old five-button row has been replaced by a collapsed review expander with:

- company selector
- decision dropdown
- Apply button
- Review with Claude button
