package templates

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"sync"
	"time"
)

// Template represents a notification email template.
type Template struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	EventType       string            `json:"event_type"`
	SubjectTemplate string            `json:"subject_template"`
	BodyHTMLOrHTML  string            `json:"body_html_template"`
	IsActive        bool              `json:"is_active"`
	CreatedAt       time.Time         `json:"created_at"`
}

// Templater manages template loading, caching, and rendering.
type Templater struct {
	store      map[string]*Template
	cache      map[string]*template.Template
	maxRetries int
	mu         sync.RWMutex
}

// NewTemplater creates a new Templater with seeded default templates.
func NewTemplater(maxRetries int) *Templater {
	t := &Templater{
		store:      make(map[string]*Template),
		cache:      make(map[string]*template.Template),
		maxRetries: maxRetries,
	}
	t.seedDefaults()
	return t
}

func (t *Templater) seedDefaults() {
	defaults := []Template{
		{
			ID:              "tmpl-payment-succeeded",
			Name:            "Order Confirmation",
			EventType:       "payment.succeeded",
			SubjectTemplate: "Your order #{{.OrderID}} has been confirmed — {{.Amount}} {{.Currency}}",
			BodyHTMLOrHTML:  `<html><body><h1>Thank you for your order</h1><p>Order ID: {{.OrderID}}</p><p>Amount: {{.Amount}} {{.Currency}}</p></body></html>`,
			IsActive:        true,
			CreatedAt:       time.Now().UTC(),
		},
		{
			ID:              "tmpl-order-cancelled",
			Name:            "Order Cancelled",
			EventType:       "order.cancelled",
			SubjectTemplate: "Your order #{{.OrderID}} has been cancelled",
			BodyHTMLOrHTML:  `<html><body><h1>Order Cancelled</h1><p>Your order #{{.OrderID}} has been cancelled.</p></body></html>`,
			IsActive:        true,
			CreatedAt:       time.Now().UTC(),
		},
		{
			ID:              "tmpl-inventory-released",
			Name:            "Stock Replenished",
			EventType:       "inventory.released",
			SubjectTemplate: "Restock notification — Item #{{.ProductID}} available again",
			BodyHTMLOrHTML:  `<html><body><h1>Restocked</h1><p>Item {{.ProductID}} is now available.</p></body></html>`,
			IsActive:        true,
			CreatedAt:       time.Now().UTC(),
		},
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for _, d := range defaults {
		t.store[d.EventType] = &d
		if err := t.cacheTemplate(d); err != nil {
			// template parse warning — will fetch on next access
		}
	}
}

func (t *Templater) cacheTemplate(tmpl Template) error {
	tmplStr := strings.Join([]string{tmpl.SubjectTemplate, tmpl.BodyHTMLOrHTML}, "\n")
	parsed, err := template.New(tmpl.ID).Parse(tmplStr)
	if err != nil {
		return fmt.Errorf("parse template %s: %w", tmpl.ID, err)
	}
	t.cache[tmpl.EventType] = parsed
	return nil
}

// GetTemplate retrieves an active template by event type.
func (t *Templater) GetTemplate(eventType string) (*Template, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if tmpl, ok := t.store[eventType]; ok && tmpl.IsActive {
		return tmpl, nil
	}
	return nil, fmt.Errorf("no active template for event type: %s", eventType)
}

// Render renders a template with the given data.
func (t *Templater) Render(eventType string, data map[string]interface{}) (string, error) {
	t.mu.RLock()
	parsed, ok := t.cache[eventType]
	t.mu.RUnlock()

	if !ok {
		tmpl, err := t.GetTemplate(eventType)
		if err != nil {
			return "", err
		}
		t.mu.Lock()
		if err := t.cacheTemplate(*tmpl); err != nil {
			t.mu.Unlock()
			return "", err
		}
		parsed = t.cache[eventType]
		t.mu.Unlock()
	}

	var buf bytes.Buffer
	if err := parsed.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render template %s: %w", eventType, err)
	}

	return buf.String(), nil
}

// UpsertTemplate adds or updates a template.
func (t *Templater) UpsertTemplate(tmpl Template) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.cacheTemplate(tmpl); err != nil {
		return err
	}

	t.store[tmpl.EventType] = &tmpl
	return nil
}

// DeleteTemplate removes a template by event type.
func (t *Templater) DeleteTemplate(eventType string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.store[eventType]; !ok {
		return false
	}

	delete(t.store, eventType)
	delete(t.cache, eventType)
	return true
}
