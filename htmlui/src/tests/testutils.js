import { fireEvent } from '@testing-library/react';

export function changeControlValue(selector, value) {
    fireEvent.change(selector, { target: { value: value } })
}

export function toggleCheckbox(selector) {
    fireEvent.click(selector)
}

export function simulateClick(selector) {
    fireEvent.click(selector);
}