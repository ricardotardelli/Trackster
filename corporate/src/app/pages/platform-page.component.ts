import { Component } from '@angular/core';

@Component({
  standalone: true,
  template: `
    <section class="section">
      <div class="container">
        <div class="section-header">
          <p class="section-kicker">Platform</p>
          <h1 class="section-title">A focused platform for structured CAN simulation workflows</h1>
          <p class="section-text">
            Trackster is built for teams that need predictable simulation control, repeatable setup, and output that can support real validation and integration work.
            The platform centers around clear configuration, DBC-aware generation, route-driven inputs, and scalable processing.
          </p>
        </div>

        <div class="card-grid three">
          <article class="surface-card block-card">
            <h3>Structured configuration</h3>
            <p>Define the key inputs that shape each generation run, from VIN patterns and DBC selection to duration, block strategy, route data, and generation mode.</p>
          </article>
          <article class="surface-card block-card">
            <h3>Route and GPS logic</h3>
            <p>Use route-aware inputs to enrich simulations with movement context, coordinate progression, and scenario-specific geographic behavior.</p>
          </article>
          <article class="surface-card block-card">
            <h3>Production-oriented delivery</h3>
            <p>Operate from a controlled browser-based environment that supports secure access and consistent workflow execution.</p>
          </article>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="container platform-layout">
        <article class="surface-card details-card">
          <h2>What the platform helps you do</h2>
          <ul>
            <li>Configure structured CAN generation runs through a unified interface</li>
            <li>Select DBC files and CAN frame subsets for targeted workflows</li>
            <li>Control simulation timing, block sizing, and generation behavior</li>
            <li>Incorporate route and speed inputs to shape output realism</li>
            <li>Generate repeatable datasets for backend, lab, or integration use</li>
          </ul>
        </article>

        <article class="surface-card details-card">
          <h2>What makes it practical</h2>
          <ul>
            <li>Clear parameterization instead of scattered manual scripts</li>
            <li>Secure browser-based access for controlled internal use</li>
            <li>Consistent workflow logic for repeatable runs</li>
            <li>Compatibility with cloud-based execution patterns</li>
            <li>A product-oriented surface instead of raw engineering plumbing</li>
          </ul>
        </article>
      </div>
    </section>

    <section class="section">
      <div class="container">
        <div class="surface-card compare-card">
          <div>
            <p class="section-kicker">Designed for clarity</p>
            <h2>Trackster turns simulation setup into a structured workflow</h2>
            <p>
              Instead of relying on disconnected tools, Trackster gives teams a more cohesive way to define, run, and repeat simulation scenarios with controlled inputs.
            </p>
          </div>

          <a class="cta-primary" href="https://studio.trackster.pt">Open Studio</a>
        </div>
      </div>
    </section>
  `,
  styles: [`
    .block-card,
    .details-card,
    .compare-card {
      padding: 24px;
    }

    .block-card h3,
    .details-card h2,
    .compare-card h2 {
      margin: 0 0 12px;
      color: #0b1f44;
    }

    .block-card p,
    .compare-card p {
      margin: 0;
      color: #475569;
      line-height: 1.75;
    }

    .platform-layout {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 22px;
    }

    .details-card ul {
      margin: 0;
      padding-left: 18px;
      color: #475569;
      line-height: 1.9;
    }

    .compare-card {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 18px;
      align-items: center;
    }

    @media (max-width: 900px) {
      .platform-layout,
      .compare-card {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class PlatformPageComponent {}
