import { Component } from '@angular/core';

@Component({
  standalone: true,
  template: `
    <section class="section">
      <div class="container">
        <div class="section-header">
          <p class="section-kicker">Use Cases</p>
          <h1 class="section-title">Use Trackster where realistic synthetic vehicle data matters</h1>
          <p class="section-text">
            Trackster is suited to teams that need more than placeholder data. It supports controlled simulation workflows for validation, backend testing, ingestion pipelines, and fleet-scale synthetic scenario creation.
          </p>
        </div>

        <div class="card-grid two">
          <article class="surface-card use-card">
            <h3>Validation and pre-integration</h3>
            <p>Prepare repeatable simulation runs for systems that need structured CAN input before full field connectivity or live environment availability.</p>
          </article>

          <article class="surface-card use-card">
            <h3>Telematics and backend ingestion testing</h3>
            <p>Feed synthetic but controlled vehicle activity into ingestion pipelines to validate behavior, transformations, storage, and downstream processing logic.</p>
          </article>

          <article class="surface-card use-card">
            <h3>Lab and partner environments</h3>
            <p>Create scenario-driven outputs that help internal labs or customer environments test data-handling workflows without relying entirely on live capture.</p>
          </article>

          <article class="surface-card use-card">
            <h3>Fleet-scale synthetic data generation</h3>
            <p>Scale from a single scenario to large synthetic populations with repeatable settings for timing, routing, selected signals, and execution strategy.</p>
          </article>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="container">
        <div class="surface-card scenario-card">
          <div class="scenario-intro">
            <p class="section-kicker">Typical scenario pattern</p>
            <h2>Define inputs, control variables, generate output</h2>
          </div>

          <div class="scenario-grid">
            <div class="scenario-step">
              <strong>1. Configure</strong>
              <p>Select DBC files, CAN frames, route settings, timing, output strategy, and vehicle parameters.</p>
            </div>
            <div class="scenario-step">
              <strong>2. Execute</strong>
              <p>Launch structured generation with secure access through a consistent browser-based workflow.</p>
            </div>
            <div class="scenario-step">
              <strong>3. Validate</strong>
              <p>Use the resulting output in downstream systems, pipelines, or validation environments.</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  `,
  styles: [`
    .use-card,
    .scenario-card {
      padding: 24px;
    }

    .use-card h3,
    .scenario-card h2,
    .scenario-step strong {
      color: #0b1f44;
    }

    .use-card h3,
    .scenario-card h2 {
      margin: 0 0 12px;
    }

    .use-card p,
    .scenario-step p {
      margin: 0;
      color: #475569;
      line-height: 1.75;
    }

    .scenario-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-top: 18px;
    }

    .scenario-step {
      padding: 18px;
      border: 1px solid rgba(191, 219, 254, 0.9);
      border-radius: 16px;
      background: linear-gradient(135deg, #ffffff, #f8fbff);
    }

    .scenario-step strong {
      display: block;
      margin-bottom: 8px;
      font-size: 1.02rem;
    }

    @media (max-width: 900px) {
      .scenario-grid {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class UseCasesPageComponent {}
