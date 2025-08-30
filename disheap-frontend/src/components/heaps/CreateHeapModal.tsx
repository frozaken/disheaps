import React from 'react';
import { Modal } from '../ui/Modal';
import { HeapForm } from './HeapForm';
import { useNotifications } from '../../store/app';
import { api } from '../../lib/api';
import { formatAPIError } from '../../lib/errors';
import type { CreateHeapFormData } from '../../lib/validation';

interface CreateHeapModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

export function CreateHeapModal({ isOpen, onClose, onSuccess }: CreateHeapModalProps) {
  const { showSuccess, showError } = useNotifications();
  const [isLoading, setIsLoading] = React.useState(false);

  const handleSubmit = async (data: CreateHeapFormData) => {
    setIsLoading(true);
    
    try {
      await api.createHeap(data);

      showSuccess(
        'Heap Created',
        `Successfully created heap "${data.topic}"`
      );
      
      onSuccess();
      onClose();
    } catch (error: any) {
      const formattedError = formatAPIError(error);
      showError(formattedError.title, formattedError.message);
      throw error; // Re-throw so form can handle it if needed
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    if (!isLoading) {
      onClose();
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title="Create New Heap"
      size="lg"
      showCloseButton={!isLoading}
    >
      <HeapForm
        mode="create"
        onSubmit={handleSubmit}
        onCancel={handleClose}
        isLoading={isLoading}
      />
    </Modal>
  );
}
