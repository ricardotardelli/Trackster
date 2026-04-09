function buildResponse(statusCode, body, origin = 'https://www.trackster.pt') {
  return {
    statusCode,
    headers: {
      'Access-Control-Allow-Origin': origin,
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'OPTIONS,POST',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  };
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export const handler = async (event) => {
  try {
    const method = event.requestContext?.http?.method || '';
    const origin = event.headers?.origin || event.headers?.Origin || '';

    const allowedOrigins = new Set([
      'https://www.trackster.pt',
      'https://trackster.pt'
    ]);

    const corsOrigin = allowedOrigins.has(origin)
      ? origin
      : 'https://www.trackster.pt';

    if (method === 'OPTIONS') {
      return buildResponse(200, { ok: true }, corsOrigin);
    }

    if (method !== 'POST') {
      return buildResponse(405, { error: 'Method not allowed' }, corsOrigin);
    }

    if (!allowedOrigins.has(origin)) {
      return buildResponse(403, { error: 'Origin not allowed' }, corsOrigin);
    }

    const body =
      typeof event.body === 'string'
        ? JSON.parse(event.body)
        : event.body || {};

    const name = String(body.name || '').trim();
    const email = String(body.email || '').trim();
    const message = String(body.message || '').trim();

    const company = String(body.company || '').trim();
    const website = String(body.website || '').trim(); // honeypot field

    if (website) {
      return buildResponse(400, { error: 'Invalid request' }, corsOrigin);
    }

    if (!name || !email || !message) {
      return buildResponse(
        400,
        { error: 'name, email and message are required' },
        corsOrigin
      );
    }

    if (!isValidEmail(email)) {
      return buildResponse(400, { error: 'Invalid email' }, corsOrigin);
    }

    if (name.length > 120) {
      return buildResponse(400, { error: 'Name is too long' }, corsOrigin);
    }

    if (email.length > 200) {
      return buildResponse(400, { error: 'Email is too long' }, corsOrigin);
    }

    if (company.length > 200) {
      return buildResponse(400, { error: 'Company is too long' }, corsOrigin);
    }

    if (message.length > 3000) {
      return buildResponse(400, { error: 'Message is too long' }, corsOrigin);
    }

    const resendResponse = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        from: 'Trackster <contact@trackster.pt>',
        to: ['contact@trackster.pt'],
        reply_to: email,
        subject: `Trackster contact from ${name}`,
        text: [
          `Name: ${name}`,
          `Company: ${company || 'Not provided'}`,
          `Email: ${email}`,
          '',
          'Message:',
          message
        ].join('\n')
      })
    });

    const resendData = await resendResponse.json();

    if (!resendResponse.ok) {
      return buildResponse(
        resendResponse.status,
        {
          error: 'Resend request failed',
          details: resendData
        },
        corsOrigin
      );
    }

    return buildResponse(
      200,
      {
        ok: true,
        message: 'Email sent successfully.'
      },
      corsOrigin
    );
  } catch (error) {
    return buildResponse(500, {
      error: error?.message || 'Unexpected error'
    });
  }
};