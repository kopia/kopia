import { fireEvent } from '@testing-library/react';

export function changeControlValue(selector, value) {
    fireEvent.change(selector, { target: { value: value } })
}

export function simulateClick(selector) {
    fireEvent.click(selector);
}